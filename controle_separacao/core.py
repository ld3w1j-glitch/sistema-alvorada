from __future__ import annotations

import os
import io
import json
import re
import sqlite3
import uuid
from contextlib import closing
from datetime import datetime
from functools import wraps
from typing import Any, Iterable

from flask import (
    Flask,
    Response,
    flash,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)
from werkzeug.security import check_password_hash, generate_password_hash

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_db_path() -> str:
    custom_db_path = os.environ.get("DB_PATH", "").strip()
    if custom_db_path:
        return custom_db_path

    volume_mount = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "").strip()
    if volume_mount:
        return os.path.join(volume_mount, "dados.db")

    return os.path.join(BASE_DIR, "dados.db")


DB_PATH = get_db_path()
STOCK_SEED_PATH = os.path.join(BASE_DIR, "stock_seed.json")
app = Flask(__name__, template_folder=os.path.join(BASE_DIR, "templates"), static_folder=os.path.join(BASE_DIR, "static"))
app.secret_key = os.environ.get("SECRET_KEY", "alvorada-separacao-lojas")


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def agora_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def agora_br() -> str:
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")


def fmt_num(value: Any) -> str:
    try:
        f = float(value or 0)
    except (TypeError, ValueError):
        return "0"
    if f.is_integer():
        return str(int(f))
    return f"{f:.3f}".rstrip("0").rstrip(".")


def fmt_money(value: Any) -> str:
    try:
        f = float(value or 0)
    except (TypeError, ValueError):
        f = 0.0
    text = f"{f:,.2f}"
    return "R$ " + text.replace(",", "X").replace(".", ",").replace("X", ".")


def parse_fator_embalagem(value: Any, field_name: str = "Fator da embalagem", default: float = 1.0) -> float:
    raw = str(value or "").strip().casefold()
    if not raw:
        return float(default)
    match = re.search(r"[-+]?\d+(?:[\.,]\d+)?", raw.replace("emb", " "))
    if not match:
        raise ValueError(f"{field_name} inválido.")
    fator = parse_float(match.group(0), field_name)
    if fator <= 0:
        raise ValueError(f"{field_name} deve ser maior que zero.")
    return fator


def quantidade_em_embalagens(quantidade_total: Any, fator_embalagem: Any) -> float:
    try:
        quantidade = float(quantidade_total or 0)
    except (TypeError, ValueError):
        quantidade = 0.0
    try:
        fator = float(fator_embalagem or 1)
    except (TypeError, ValueError):
        fator = 1.0
    if fator <= 0:
        fator = 1.0
    return quantidade / fator


def fmt_fator_embalagem(value: Any) -> str:
    return f"Emb{fmt_num(value or 1)}"


ACCESS_OPTIONS: list[tuple[str, str]] = [
    ("painel", "Painel"),
    ("separacoes", "Separações"),
    ("estoque", "Estoque"),
    ("relatorios", "Relatórios"),
    ("usuarios", "Usuários"),
    ("lojas", "Lojas"),
    ("pedidos", "Criar pedidos"),
    ("lotes", "Lotes"),
    ("configuracoes", "Configurações"),
]
ACCESS_KEYS = {key for key, _ in ACCESS_OPTIONS}
ACCESS_LABELS = dict(ACCESS_OPTIONS)
PERMISSION_LEVEL_LABELS = {"admin": "Admin", "comum": "Comum"}
ROLE_LABELS = {
    "admin": "Admin",
    "separador": "Separador",
    "conferente": "Conferente",
    "balanco": "Balanço",
}
DEFAULT_ACCESS_BY_ROLE: dict[str, set[str]] = {
    "admin": set(ACCESS_KEYS),
    "separador": {"painel", "separacoes"},
    "conferente": {"painel", "separacoes"},
    "balanco": {"painel", "estoque", "relatorios"},
}
MODULE_ENDPOINTS = {
    "painel": "dashboard",
    "separacoes": "listar_separacoes",
    "estoque": "estoque",
    "relatorios": "relatorios",
    "usuarios": "usuarios",
    "lojas": "lojas",
    "pedidos": "nova_separacao",
    "lotes": "listar_lotes",
    "configuracoes": "configuracoes",
}
STOCK_MOVEMENT_TYPE_OPTIONS: list[tuple[str, str]] = [
    ("ENTRADA_INICIAL", "Cadastro inicial"),
    ("AJUSTE_MANUAL", "Ajuste manual"),
    ("RECONTAGEM", "Ajuste de quantidade"),
    ("REMOVIDO_ESTOQUE", "Remoção do estoque"),
    ("SAIDA_SEPARACAO", "Saída por separação"),
    ("ESTORNO_HISTORICO", "Estorno do histórico"),
]


def normalize_role(value: Any) -> str:
    role = str(value or "").strip().lower()
    return role if role in {"admin", "separador", "conferente", "balanco"} else "separador"



def normalize_permission_level(value: Any, role: Any = None) -> str:
    if normalize_role(role) == "admin":
        return "admin"
    level = str(value or "").strip().lower()
    if level in {"admin", "comum"}:
        return level
    return "comum"



def default_access_rules(role: Any, permission_level: Any = "comum") -> set[str]:
    role_norm = normalize_role(role)
    level_norm = normalize_permission_level(permission_level, role_norm)
    if level_norm == "admin":
        return set(ACCESS_KEYS)
    return set(DEFAULT_ACCESS_BY_ROLE.get(role_norm, {"painel"}))



def serialize_access_rules(accesses: Iterable[str]) -> str:
    normalized = sorted({str(item).strip().lower() for item in accesses if str(item).strip().lower() in ACCESS_KEYS})
    return json.dumps(normalized, ensure_ascii=False)



def parse_access_rules(raw: Any, role: Any = None, permission_level: Any = "comum") -> set[str]:
    if isinstance(raw, (list, tuple, set)):
        valores = raw
    else:
        texto = str(raw or "").strip()
        if not texto:
            return default_access_rules(role, permission_level)
        try:
            valores = json.loads(texto)
        except json.JSONDecodeError:
            return default_access_rules(role, permission_level)
    acessos = {str(item).strip().lower() for item in valores if str(item).strip().lower() in ACCESS_KEYS}
    if not acessos:
        return default_access_rules(role, permission_level)
    return acessos



def user_permission_level(user: sqlite3.Row | dict[str, Any] | None) -> str:
    if user is None:
        return "comum"
    role = user["role"] if "role" in user.keys() else None
    raw = user["permission_level"] if "permission_level" in user.keys() else None
    return normalize_permission_level(raw, role)



def user_is_admin(user: sqlite3.Row | dict[str, Any] | None) -> bool:
    if user is None:
        return False
    role = normalize_role(user["role"] if "role" in user.keys() else None)
    return role == "admin" or user_permission_level(user) == "admin"



def user_access_set(user: sqlite3.Row | dict[str, Any] | None) -> set[str]:
    if user is None:
        return set()
    role = user["role"] if "role" in user.keys() else None
    if user_is_admin(user):
        return set(ACCESS_KEYS)
    raw = user["access_rules"] if "access_rules" in user.keys() else None
    return parse_access_rules(raw, role, user_permission_level(user))



def user_has_access(user: sqlite3.Row | dict[str, Any] | None, module: str) -> bool:
    module_key = str(module or "").strip().lower()
    if module_key not in ACCESS_KEYS:
        return False
    return module_key in user_access_set(user)



def access_labels_for_user(user: sqlite3.Row | dict[str, Any] | None) -> list[str]:
    return [ACCESS_LABELS[key] for key in ACCESS_OPTIONS_KEYS_IN_ORDER if key in user_access_set(user)]


ACCESS_OPTIONS_KEYS_IN_ORDER = [key for key, _ in ACCESS_OPTIONS]



def role_label(role: Any) -> str:
    return ROLE_LABELS.get(normalize_role(role), str(role or "-").strip() or "-")



def permission_level_label(value: Any, role: Any = None) -> str:
    level = normalize_permission_level(value, role)
    return PERMISSION_LEVEL_LABELS.get(level, level.title())



def first_allowed_endpoint(user: sqlite3.Row | dict[str, Any] | None) -> str:
    for module in ACCESS_OPTIONS_KEYS_IN_ORDER:
        if user_has_access(user, module):
            return MODULE_ENDPOINTS[module]
    return "minha_conta"



def forbidden_redirect(message: str) -> Response:
    flash(message, "error")
    destino = first_allowed_endpoint(g.user)
    if request.endpoint == destino:
        destino = "minha_conta"
    return redirect(url_for(destino))



def module_required(module: str):
    def decorator(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            if g.user is None:
                return redirect(url_for("login"))
            if not user_has_access(g.user, module):
                return forbidden_redirect("Você não tem permissão para acessar essa área.")
            return view(*args, **kwargs)

        return wrapped

    return decorator



def can_adjust_stock(user: sqlite3.Row | dict[str, Any] | None) -> bool:
    if user is None:
        return False
    return user_has_access(user, "estoque")



def can_edit_stock_registration(user: sqlite3.Row | dict[str, Any] | None) -> bool:
    if user is None:
        return False
    return user_has_access(user, "estoque") and user_is_admin(user)



def count_admin_users(conn: sqlite3.Connection, exclude_user_id: int | None = None) -> int:
    rows = conn.execute("SELECT id, role, permission_level FROM users WHERE ativo = 1").fetchall()
    total = 0
    for row in rows:
        if exclude_user_id is not None and int(row["id"]) == int(exclude_user_id):
            continue
        if user_is_admin(row):
            total += 1
    return total


def natural_store_sort_key(value: Any) -> tuple[Any, ...]:
    text = str(value or '').strip()
    parts = re.split(r'(\d+)', text.casefold())
    key: list[Any] = []
    for part in parts:
        if not part:
            continue
        if part.isdigit():
            key.append((0, int(part)))
        else:
            key.append((1, part))
    return tuple(key)


def sort_store_rows(rows: Iterable[sqlite3.Row]) -> list[sqlite3.Row]:
    return sorted(rows, key=lambda row: (natural_store_sort_key(row['store_nome']), row['id']))


def ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in columns:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def ensure_schema_updates(conn: sqlite3.Connection) -> None:
    ensure_column(conn, "separations", "lote_codigo", "lote_codigo TEXT")
    ensure_column(conn, "stock_items", "ativo", "ativo INTEGER NOT NULL DEFAULT 1")
    ensure_column(conn, "stock_items", "codigo_barras", "codigo_barras TEXT")
    ensure_column(conn, "stock_items", "fator_embalagem", "fator_embalagem REAL NOT NULL DEFAULT 1")
    ensure_column(conn, "separation_items", "fator_embalagem", "fator_embalagem REAL NOT NULL DEFAULT 1")
    ensure_column(conn, "separation_items", "carryover_source_item_id", "carryover_source_item_id INTEGER")
    ensure_column(conn, "separation_items", "carryover_copied", "carryover_copied INTEGER NOT NULL DEFAULT 0")
    ensure_column(conn, "separation_items", "quantidade_conferida", "quantidade_conferida REAL")
    ensure_column(conn, "separation_items", "conferido_em", "conferido_em TEXT")
    ensure_column(conn, "users", "permission_level", "permission_level TEXT NOT NULL DEFAULT 'comum'")
    ensure_column(conn, "users", "access_rules", "access_rules TEXT NOT NULL DEFAULT ''")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_separations_lote_codigo ON separations(lote_codigo)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stock_items_codigo_barras ON stock_items(codigo_barras)")
    conn.execute("UPDATE separations SET lote_codigo = 'SEP-' || id WHERE lote_codigo IS NULL OR TRIM(lote_codigo) = ''")
    conn.execute("UPDATE stock_items SET fator_embalagem = 1 WHERE fator_embalagem IS NULL OR fator_embalagem <= 0")
    conn.execute("UPDATE separation_items SET fator_embalagem = 1 WHERE fator_embalagem IS NULL OR fator_embalagem <= 0")
    conn.execute("UPDATE separation_items SET carryover_copied = 0 WHERE carryover_copied IS NULL")
    users = conn.execute("SELECT id, role, permission_level, access_rules FROM users").fetchall()
    for user in users:
        permission_level = normalize_permission_level(user["permission_level"], user["role"])
        access_rules = parse_access_rules(user["access_rules"], user["role"], permission_level)
        conn.execute(
            "UPDATE users SET permission_level = ?, access_rules = ? WHERE id = ?",
            (permission_level, serialize_access_rules(access_rules), user["id"]),
        )


def novo_lote_codigo() -> str:
    return "LT-" + uuid.uuid4().hex[:10].upper()


def lote_operacao_chave_expr(alias: str = "s") -> str:
    return f"""
    CASE
        WHEN {alias}.lote_codigo LIKE 'SEP-%' THEN
            'LEGADO~' || {alias}.lote_nome || '~' || {alias}.data_referencia || '~' ||
            COALESCE(CAST({alias}.responsavel_id AS TEXT), '0') || '~' ||
            COALESCE(CAST({alias}.conferente_id AS TEXT), '0') || '~' ||
            COALESCE(CAST({alias}.criado_por AS TEXT), '0') || '~' ||
            COALESCE(CAST({alias}.usar_estoque AS TEXT), '0') || '~' ||
            COALESCE(substr({alias}.criado_em, 1, 16), '')
        ELSE COALESCE(NULLIF(TRIM({alias}.lote_codigo), ''),
            'LEGADO~' || {alias}.lote_nome || '~' || {alias}.data_referencia || '~' ||
            COALESCE(CAST({alias}.responsavel_id AS TEXT), '0') || '~' ||
            COALESCE(CAST({alias}.conferente_id AS TEXT), '0') || '~' ||
            COALESCE(CAST({alias}.criado_por AS TEXT), '0') || '~' ||
            COALESCE(CAST({alias}.usar_estoque AS TEXT), '0') || '~' ||
            COALESCE(substr({alias}.criado_em, 1, 16), '')
        )
    END
    """


def lote_operacao_chave_row(row: sqlite3.Row | dict[str, Any]) -> str:
    lote_codigo = (row["lote_codigo"] or "").strip() if "lote_codigo" in row.keys() else ""
    if lote_codigo and not lote_codigo.startswith("SEP-"):
        return lote_codigo
    return "LEGADO~{lote_nome}~{data_referencia}~{responsavel_id}~{conferente_id}~{criado_por}~{usar_estoque}~{criado_minuto}".format(
        lote_nome=row["lote_nome"],
        data_referencia=row["data_referencia"],
        responsavel_id=row["responsavel_id"] or 0,
        conferente_id=row["conferente_id"] or 0,
        criado_por=row["criado_por"] or 0,
        usar_estoque=row["usar_estoque"] or 0,
        criado_minuto=(row["criado_em"] or "")[:16],
    )


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nome TEXT NOT NULL,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL,
    permission_level TEXT NOT NULL DEFAULT 'comum',
    access_rules TEXT NOT NULL DEFAULT '',
    ativo INTEGER NOT NULL DEFAULT 1,
    criado_em TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nome TEXT NOT NULL UNIQUE,
    ativo INTEGER NOT NULL DEFAULT 1,
    criado_em TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stock_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    codigo TEXT NOT NULL UNIQUE,
    codigo_barras TEXT,
    descricao TEXT NOT NULL,
    fator_embalagem REAL NOT NULL DEFAULT 1,
    quantidade_atual REAL NOT NULL DEFAULT 0,
    custo_unitario REAL NOT NULL DEFAULT 0,
    ativo INTEGER NOT NULL DEFAULT 1,
    atualizado_em TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stock_movements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_item_id INTEGER NOT NULL,
    tipo TEXT NOT NULL,
    quantidade REAL NOT NULL,
    observacao TEXT,
    referencia_tipo TEXT,
    referencia_id INTEGER,
    criado_por INTEGER,
    criado_em TEXT NOT NULL,
    FOREIGN KEY (stock_item_id) REFERENCES stock_items (id),
    FOREIGN KEY (criado_por) REFERENCES users (id)
);

CREATE TABLE IF NOT EXISTS separations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lote_nome TEXT NOT NULL,
    data_referencia TEXT NOT NULL,
    store_id INTEGER NOT NULL,
    responsavel_id INTEGER,
    conferente_id INTEGER,
    status TEXT NOT NULL DEFAULT 'ABERTA',
    usar_estoque INTEGER NOT NULL DEFAULT 1,
    observacao TEXT,
    criado_por INTEGER,
    criado_em TEXT NOT NULL,
    enviado_conferencia_em TEXT,
    finalizado_em TEXT,
    FOREIGN KEY (store_id) REFERENCES stores (id),
    FOREIGN KEY (responsavel_id) REFERENCES users (id),
    FOREIGN KEY (conferente_id) REFERENCES users (id),
    FOREIGN KEY (criado_por) REFERENCES users (id)
);

CREATE TABLE IF NOT EXISTS separation_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    separation_id INTEGER NOT NULL,
    codigo TEXT NOT NULL,
    descricao TEXT NOT NULL,
    fator_embalagem REAL NOT NULL DEFAULT 1,
    quantidade_pedida REAL NOT NULL,
    quantidade_separada REAL NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'PENDENTE',
    custo_unitario_ref REAL NOT NULL DEFAULT 0,
    carryover_source_item_id INTEGER,
    carryover_copied INTEGER NOT NULL DEFAULT 0,
    criado_em TEXT NOT NULL,
    atualizado_em TEXT NOT NULL,
    FOREIGN KEY (separation_id) REFERENCES separations (id) ON DELETE CASCADE
);
"""




def carregar_seed_estoque() -> dict[str, Any] | None:
    if not os.path.exists(STOCK_SEED_PATH):
        return None
    try:
        with open(STOCK_SEED_PATH, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def garantir_produtos_seed(conn: sqlite3.Connection) -> None:
    payload = carregar_seed_estoque()
    if not payload:
        return

    version = str(payload.get("version") or "").strip()
    items = payload.get("items") or []
    if not version or not isinstance(items, list):
        return

    atual = conn.execute(
        "SELECT value FROM settings WHERE key = ?",
        ("stock_seed_version",),
    ).fetchone()
    if atual and (atual["value"] or "") == version:
        return

    rows: list[tuple[str, str, str, float, float, float, str]] = []
    agora = agora_iso()

    def normalizar_texto(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        text = str(value).strip()
        if text.endswith(".0"):
            text = text[:-2]
        return text

    for item in items:
        if not isinstance(item, dict):
            continue
        codigo = normalizar_texto(item.get("codigo"))
        descricao = str(item.get("descricao") or "").strip()
        codigo_barras = normalizar_texto(item.get("codigo_barras"))
        if not codigo or not descricao:
            continue
        try:
            fator_embalagem = float(item.get("fator_embalagem") or 1)
        except (TypeError, ValueError):
            fator_embalagem = 1.0
        try:
            quantidade_atual = float(item.get("quantidade_atual") or 0)
        except (TypeError, ValueError):
            quantidade_atual = 0.0
        try:
            custo_unitario = float(item.get("custo_unitario") or 0)
        except (TypeError, ValueError):
            custo_unitario = 0.0
        if fator_embalagem <= 0:
            fator_embalagem = 1.0
        rows.append((codigo, codigo_barras, descricao, fator_embalagem, quantidade_atual, custo_unitario, agora))

    if rows:
        conn.executemany(
            """
            INSERT INTO stock_items (codigo, codigo_barras, descricao, fator_embalagem, quantidade_atual, custo_unitario, ativo, atualizado_em)
            VALUES (?, ?, ?, ?, ?, ?, 1, ?)
            ON CONFLICT(codigo) DO UPDATE SET
                descricao = excluded.descricao,
                codigo_barras = COALESCE(NULLIF(excluded.codigo_barras, ''), stock_items.codigo_barras),
                ativo = 1,
                fator_embalagem = CASE
                    WHEN stock_items.fator_embalagem IS NULL OR stock_items.fator_embalagem <= 0 THEN excluded.fator_embalagem
                    ELSE stock_items.fator_embalagem
                END,
                quantidade_atual = CASE
                    WHEN ABS(excluded.quantidade_atual) > 0.000001 THEN excluded.quantidade_atual
                    ELSE stock_items.quantidade_atual
                END,
                custo_unitario = CASE
                    WHEN ABS(excluded.custo_unitario) > 0.000001 THEN excluded.custo_unitario
                    ELSE stock_items.custo_unitario
                END,
                atualizado_em = CASE
                    WHEN stock_items.ativo = 0 OR ABS(excluded.quantidade_atual) > 0.000001 OR ABS(excluded.custo_unitario) > 0.000001 THEN excluded.atualizado_em
                    ELSE stock_items.atualizado_em
                END
            """,
            rows,
        )

    conn.execute(
        "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        ("stock_seed_version", version),
    )


def get_setting(key: str, default: str = "") -> str:
    with closing(get_conn()) as conn:
        row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(key: str, value: str) -> None:
    with closing(get_conn()) as conn:
        conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )
        conn.commit()


def query_one(sql: str, params: Iterable[Any] = ()) -> sqlite3.Row | None:
    with closing(get_conn()) as conn:
        return conn.execute(sql, tuple(params)).fetchone()


def query_all(sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
    with closing(get_conn()) as conn:
        return conn.execute(sql, tuple(params)).fetchall()


def ensure_default_data() -> None:
    with closing(get_conn()) as conn:
        conn.executescript(SCHEMA_SQL)
        ensure_schema_updates(conn)
        garantir_produtos_seed(conn)
        conn.execute(
            "INSERT OR IGNORE INTO settings (key, value) VALUES ('vincular_estoque', '1')"
        )
        conn.commit()


ensure_default_data()


@app.before_request
def bootstrap() -> None:
    g.user = current_user()


def current_user() -> sqlite3.Row | None:
    user_id = session.get("user_id")
    if not user_id:
        return None
    with closing(get_conn()) as conn:
        return conn.execute(
            "SELECT * FROM users WHERE id = ? AND ativo = 1", (user_id,)
        ).fetchone()


def login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if g.user is None:
            return redirect(url_for("login"))
        return view(*args, **kwargs)

    return wrapped


def roles_required(*roles: str):
    normalized_roles = {normalize_role(role) for role in roles}

    def decorator(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            if g.user is None:
                return redirect(url_for("login"))
            if user_is_admin(g.user):
                return view(*args, **kwargs)
            if normalize_role(g.user["role"]) not in normalized_roles:
                return forbidden_redirect("Você não tem permissão para acessar essa área.")
            return view(*args, **kwargs)

        return wrapped

    return decorator


def parse_float(raw: str, field_name: str) -> float:
    valor = (raw or "").strip().replace(".", "").replace(",", ".") if "," in (raw or "") and (raw or "").count(",") == 1 and (raw or "").count(".") >= 1 else (raw or "").strip().replace(",", ".")
    try:
        number = float(valor)
    except ValueError as exc:
        raise ValueError(f"{field_name} inválido.") from exc
    if number < 0:
        raise ValueError(f"{field_name} não pode ser negativo.")
    return number


def role_badge(role: str) -> str:
    classes = {
        "admin": "badge badge-admin",
        "separador": "badge badge-separador",
        "conferente": "badge badge-conferente",
        "balanco": "badge badge-admin",
    }
    return classes.get(role, "badge")


def status_class(status: str) -> str:
    normalized = status.lower().replace(" ", "_")
    return f"badge status-{normalized}"


def stock_movement_label(value: Any) -> str:
    labels = {
        "ENTRADA_INICIAL": "Cadastro inicial",
        "AJUSTE_MANUAL": "Ajuste manual",
        "RECONTAGEM": "Ajuste de quantidade",
        "REMOVIDO_ESTOQUE": "Remoção do estoque",
        "SAIDA_SEPARACAO": "Saída por separação",
        "ESTORNO_HISTORICO": "Estorno do histórico",
    }
    key = str(value or "").strip().upper()
    return labels.get(key, key.replace("_", " ").title() or "Movimentação")


def sanitize_stock_history_filters(args: Any) -> dict[str, str]:
    termo = str(args.get("q", "") or "").strip()
    hist_usuario = str(args.get("hist_usuario", "") or "").strip()
    hist_tipo = str(args.get("hist_tipo", "") or "").strip().upper()
    hist_data_inicial = str(args.get("hist_data_inicial", "") or "").strip()
    hist_data_final = str(args.get("hist_data_final", "") or "").strip()

    if not hist_usuario.isdigit():
        hist_usuario = ""
    if hist_tipo not in {key for key, _ in STOCK_MOVEMENT_TYPE_OPTIONS}:
        hist_tipo = ""
    return {
        "q": termo,
        "hist_usuario": hist_usuario,
        "hist_tipo": hist_tipo,
        "hist_data_inicial": hist_data_inicial,
        "hist_data_final": hist_data_final,
    }


def build_stock_history_query(filters: dict[str, str], limit: int | None = 80) -> tuple[str, list[Any]]:
    movement_filters: list[str] = []
    movement_params: list[Any] = []
    termo = filters["q"]
    if termo:
        movement_filters.append("(si.codigo = ? OR si.codigo_barras = ? OR si.codigo LIKE ? OR si.descricao LIKE ? OR si.codigo_barras LIKE ?)")
        movement_like = f"%{termo}%"
        movement_params.extend([termo, termo, movement_like, movement_like, movement_like])
    if filters["hist_usuario"]:
        movement_filters.append("sm.criado_por = ?")
        movement_params.append(int(filters["hist_usuario"]))
    if filters["hist_tipo"]:
        movement_filters.append("sm.tipo = ?")
        movement_params.append(filters["hist_tipo"])
    if filters["hist_data_inicial"]:
        movement_filters.append("date(sm.criado_em) >= date(?)")
        movement_params.append(filters["hist_data_inicial"])
    if filters["hist_data_final"]:
        movement_filters.append("date(sm.criado_em) <= date(?)")
        movement_params.append(filters["hist_data_final"])

    movement_where = "WHERE " + " AND ".join(movement_filters) if movement_filters else ""
    sql = f"""
        SELECT sm.*, si.codigo, si.codigo_barras, si.descricao,
               u.nome AS usuario_nome,
               u.username AS usuario_login
        FROM stock_movements sm
        JOIN stock_items si ON si.id = sm.stock_item_id
        LEFT JOIN users u ON u.id = sm.criado_por
        {movement_where}
        ORDER BY sm.id DESC
    """
    if limit is not None:
        sql += f"\n        LIMIT {int(limit)}"
    return sql, movement_params


def fetch_stock_movements(filters: dict[str, str], limit: int | None = 80) -> list[sqlite3.Row]:
    sql, params = build_stock_history_query(filters, limit=limit)
    return query_all(sql, params)


def stock_history_filter_labels(filters: dict[str, str]) -> list[str]:
    labels: list[str] = []
    if filters["q"]:
        labels.append(f"Busca: {filters['q']}")
    if filters["hist_usuario"]:
        usuario = query_one("SELECT nome, username FROM users WHERE id = ?", (int(filters["hist_usuario"]),))
        if usuario:
            nome = usuario["nome"] or usuario["username"] or "Usuário"
            if usuario["username"] and usuario["username"] != nome:
                nome = f"{nome} ({usuario['username']})"
            labels.append(f"Usuário: {nome}")
    if filters["hist_tipo"]:
        labels.append(f"Tipo: {stock_movement_label(filters['hist_tipo'])}")
    if filters["hist_data_inicial"]:
        labels.append(f"Data inicial: {filters['hist_data_inicial']}")
    if filters["hist_data_final"]:
        labels.append(f"Data final: {filters['hist_data_final']}")
    return labels


def stock_history_export_filename(extensao: str) -> str:
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return f"historico-estoque-{stamp}.{extensao}"


app.jinja_env.globals.update(
    fmt_num=fmt_num,
    fmt_money=fmt_money,
    fmt_fator_embalagem=fmt_fator_embalagem,
    quantidade_em_embalagens=quantidade_em_embalagens,
    role_badge=role_badge,
    role_label=role_label,
    permission_level_label=permission_level_label,
    user_is_admin=user_is_admin,
    user_has_access=user_has_access,
    user_access_set=user_access_set,
    access_labels_for_user=access_labels_for_user,
    access_options=ACCESS_OPTIONS,
    permission_level_options=[("comum", "Comum"), ("admin", "Admin")],
    status_class=status_class,
    lote_operacao_chave_row=lote_operacao_chave_row,
    can_adjust_stock=can_adjust_stock,
    can_edit_stock_registration=can_edit_stock_registration,
    stock_movement_label=stock_movement_label,
)




@app.route("/login", methods=["GET", "POST"])
def login() -> str | Response:
    if g.user is not None:
        return redirect(url_for("dashboard"))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        user = query_one(
            "SELECT * FROM users WHERE username = ? AND ativo = 1", (username,)
        )
        if user is None or not check_password_hash(user["password_hash"], password):
            flash("Usuário ou senha inválidos.", "error")
            return render_template("login.html", title="Login")

        session["user_id"] = user["id"]
        flash("Login realizado com sucesso.", "success")
        return redirect(url_for(first_allowed_endpoint(user)))

    return render_template("login.html", title="Login")


@app.get("/logout")
def logout() -> Response:
    session.clear()
    flash("Sessão encerrada.", "success")
    return redirect(url_for("login"))


@app.route("/minha-conta", methods=["GET", "POST"])
@login_required
def minha_conta() -> str | Response:
    if request.method == "POST":
        senha_atual = request.form.get("senha_atual", "")
        nova_senha = request.form.get("nova_senha", "")
        confirmar_senha = request.form.get("confirmar_senha", "")

        if not check_password_hash(g.user["password_hash"], senha_atual):
            flash("A senha atual está incorreta.", "error")
            return redirect(url_for("minha_conta"))
        if len(nova_senha) < 4:
            flash("A nova senha precisa ter pelo menos 4 caracteres.", "error")
            return redirect(url_for("minha_conta"))
        if nova_senha != confirmar_senha:
            flash("A confirmação da nova senha não confere.", "error")
            return redirect(url_for("minha_conta"))
        if check_password_hash(g.user["password_hash"], nova_senha):
            flash("Escolha uma senha diferente da atual.", "error")
            return redirect(url_for("minha_conta"))

        with closing(get_conn()) as conn:
            conn.execute(
                "UPDATE users SET password_hash = ? WHERE id = ?",
                (generate_password_hash(nova_senha), g.user["id"]),
            )
            conn.commit()

        flash("Senha alterada com sucesso.", "success")
        return redirect(url_for("minha_conta"))

    return render_template("minha_conta.html", title="Minha conta")


def ultimos_lotes_resumo(limit: int = 8, include_canceladas: bool = True) -> list[sqlite3.Row]:
    chave_expr = lote_operacao_chave_expr("s")
    where_parts = ["1=1"]
    if not include_canceladas:
        where_parts.append("s.status <> 'CANCELADA'")
    if normalize_role(g.user["role"]) == "separador" and not user_is_admin(g.user):
        where_parts.append("s.responsavel_id = ?")
        params: list[Any] = [g.user["id"], limit]
    elif normalize_role(g.user["role"]) == "conferente" and not user_is_admin(g.user):
        where_parts.append("s.conferente_id = ?")
        params = [g.user["id"], limit]
    else:
        params = [limit]
    where = " AND ".join(where_parts)
    return query_all(
        f"""
        SELECT {chave_expr} AS operacao_chave,
               s.lote_nome,
               s.data_referencia,
               MAX(COALESCE(s.finalizado_em, s.criado_em)) AS data_evento,
               MAX(r.nome) AS responsavel_nome,
               MAX(c.nome) AS conferente_nome,
               COUNT(*) AS total_lojas,
               GROUP_CONCAT(st.nome, ' • ') AS lojas,
               CASE
                   WHEN SUM(CASE WHEN s.status = 'FINALIZADA' THEN 1 ELSE 0 END) > 0 THEN 'FINALIZADA'
                   WHEN SUM(CASE WHEN s.status = 'AGUARDANDO_CONFERENCIA' THEN 1 ELSE 0 END) > 0 THEN 'AGUARDANDO_CONFERENCIA'
                   WHEN SUM(CASE WHEN s.status = 'EM_SEPARACAO' THEN 1 ELSE 0 END) > 0 THEN 'EM_SEPARACAO'
                   WHEN SUM(CASE WHEN s.status = 'ABERTA' THEN 1 ELSE 0 END) > 0 THEN 'ABERTA'
                   ELSE 'CANCELADA'
               END AS status_resumo
        FROM separations s
        JOIN stores st ON st.id = s.store_id
        LEFT JOIN users r ON r.id = s.responsavel_id
        LEFT JOIN users c ON c.id = s.conferente_id
        WHERE {where}
        GROUP BY operacao_chave, s.lote_nome, s.data_referencia
        ORDER BY MAX(COALESCE(s.finalizado_em, s.criado_em)) DESC, MAX(s.id) DESC
        LIMIT ?
        """,
        params,
    )


def carregar_lote_completo(operacao_chave: str) -> list[sqlite3.Row]:
    chave_expr = lote_operacao_chave_expr("s")
    rows = query_all(
        f"""
        SELECT s.*, st.nome AS store_nome,
               r.nome AS responsavel_nome,
               c.nome AS conferente_nome,
               {chave_expr} AS operacao_chave
        FROM separations s
        JOIN stores st ON st.id = s.store_id
        LEFT JOIN users r ON r.id = s.responsavel_id
        LEFT JOIN users c ON c.id = s.conferente_id
        WHERE {chave_expr} = ?
        ORDER BY s.id ASC
        """,
        (operacao_chave,),
    )
    return sort_store_rows(rows)


def excluir_separacao_cancelada_no_conn(conn: sqlite3.Connection, separation_id: int) -> None:
    separation = conn.execute("SELECT id, status FROM separations WHERE id = ?", (separation_id,)).fetchone()
    if separation is None:
        raise ValueError("Separação não encontrada.")
    if separation["status"] != "CANCELADA":
        raise ValueError("Só é possível excluir de vez uma separação cancelada.")
    conn.execute("DELETE FROM separation_items WHERE separation_id = ?", (separation_id,))
    conn.execute("DELETE FROM separations WHERE id = ?", (separation_id,))


def apagar_historico_separacao_no_conn(conn: sqlite3.Connection, separation_id: int, actor_id: int) -> None:
    separation = conn.execute(
        """
        SELECT s.*, st.nome AS store_nome
        FROM separations s
        JOIN stores st ON st.id = s.store_id
        WHERE s.id = ?
        """,
        (separation_id,),
    ).fetchone()
    if separation is None:
        raise ValueError("Separação não encontrada.")
    if separation["status"] != "FINALIZADA":
        raise ValueError("Só é possível apagar do histórico uma separação finalizada.")

    usar_controle_global = get_setting("vincular_estoque", "1") == "1"
    precisa_estornar = usar_controle_global and bool(separation["usar_estoque"])
    itens = conn.execute("SELECT * FROM separation_items WHERE separation_id = ?", (separation_id,)).fetchall()
    if precisa_estornar:
        for item in itens:
            stock = conn.execute("SELECT * FROM stock_items WHERE codigo = ?", (item["codigo"],)).fetchone()
            if stock is None:
                conn.execute(
                    "INSERT INTO stock_items (codigo, descricao, quantidade_atual, custo_unitario, ativo, atualizado_em) VALUES (?, ?, 0, ?, 1, ?)",
                    (item["codigo"], item["descricao"], item["custo_unitario_ref"], agora_iso()),
                )
                stock = conn.execute("SELECT * FROM stock_items WHERE codigo = ?", (item["codigo"],)).fetchone()
            novo_saldo = float(stock["quantidade_atual"]) + float(item["quantidade_separada"])
            conn.execute(
                "UPDATE stock_items SET quantidade_atual = ?, ativo = 1, atualizado_em = ? WHERE id = ?",
                (novo_saldo, agora_iso(), stock["id"]),
            )
            conn.execute(
                "INSERT INTO stock_movements (stock_item_id, tipo, quantidade, observacao, referencia_tipo, referencia_id, criado_por, criado_em) VALUES (?, 'ESTORNO_HISTORICO', ?, ?, 'SEPARACAO', ?, ?, ?)",
                (stock["id"], float(item["quantidade_separada"]), f"Estorno do histórico da separação {separation['lote_nome']} - {separation['store_nome']}", separation_id, actor_id, agora_iso()),
            )
    observacao_atual = (separation["observacao"] or "").strip()
    nota_cancelamento = f"Histórico removido pelo admin em {agora_br()}."
    nova_observacao = (observacao_atual + "\n" + nota_cancelamento).strip() if observacao_atual else nota_cancelamento
    conn.execute(
        "UPDATE separations SET status = 'CANCELADA', finalizado_em = NULL, enviado_conferencia_em = NULL, observacao = ? WHERE id = ?",
        (nova_observacao, separation_id),
    )


def dashboard_stats() -> dict[str, Any]:
    user = g.user
    where_clauses = ["s.status <> 'CANCELADA'"]
    params: list[Any] = []
    if user and normalize_role(user["role"]) == "separador" and not user_is_admin(user):
        where_clauses.append("s.responsavel_id = ?")
        params.append(user["id"])
    elif user and normalize_role(user["role"]) == "conferente" and not user_is_admin(user):
        where_clauses.append("s.conferente_id = ?")
        params.append(user["id"])
    where = "WHERE " + " AND ".join(where_clauses)

    resumo = query_one(
        f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN status = 'ABERTA' THEN 1 ELSE 0 END) AS abertas,
            SUM(CASE WHEN status = 'AGUARDANDO_CONFERENCIA' THEN 1 ELSE 0 END) AS aguardando,
            SUM(CASE WHEN status = 'FINALIZADA' THEN 1 ELSE 0 END) AS finalizadas
        FROM separations s
        {where}
        """,
        params,
    )
    estoque = query_one(
        "SELECT COUNT(*) AS itens, COALESCE(SUM(quantidade_atual), 0) AS total_quantidade FROM stock_items"
    )
    return {
        "total": resumo["total"] if resumo else 0,
        "abertas": resumo["abertas"] if resumo else 0,
        "aguardando": resumo["aguardando"] if resumo else 0,
        "finalizadas": resumo["finalizadas"] if resumo else 0,
        "itens_estoque": estoque["itens"] if estoque else 0,
        "qtd_estoque": estoque["total_quantidade"] if estoque else 0,
        "vincular_estoque": get_setting("vincular_estoque", "1") == "1",
        "usar_conferente": get_setting("usar_conferente", "1") == "1",
    }




@app.get("/")
@login_required
@module_required("painel")
def dashboard() -> str:
    lojas_ativas = query_one("SELECT COUNT(*) AS c FROM stores WHERE ativo = 1")["c"]
    usuarios_ativos = query_one("SELECT COUNT(*) AS c FROM users WHERE ativo = 1")["c"]
    finalizadas_hoje = query_one(
        "SELECT COUNT(*) AS c FROM separations WHERE finalizado_em LIKE ?", (datetime.now().strftime("%Y-%m-%d") + "%",)
    )["c"]
    ultimos_lotes = ultimos_lotes_resumo(8, include_canceladas=True)
    return render_template(
        "dashboard.html",
        title="Painel",
        stats=dashboard_stats(),
        ultimos_lotes=ultimos_lotes,
        lojas_ativas=lojas_ativas,
        usuarios_ativos=usuarios_ativos,
        finalizadas_hoje=finalizadas_hoje,
    )


@app.route("/usuarios", methods=["GET", "POST"])
@login_required
@module_required("usuarios")
@roles_required("admin")
def usuarios() -> str | Response:
    if request.method == "POST":
        nome = request.form.get("nome", "").strip()
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        role = normalize_role(request.form.get("role", "separador"))
        permission_level = normalize_permission_level(request.form.get("permission_level", "comum"), role)
        access_rules = set(request.form.getlist("access_rules"))
        if permission_level == "admin":
            access_rules = set(ACCESS_KEYS)
        else:
            access_rules = {item for item in access_rules if item in ACCESS_KEYS}
        if not nome or not username or not password:
            flash("Preencha os dados do usuário corretamente.", "error")
            return redirect(url_for("usuarios"))
        if permission_level != "admin" and not access_rules:
            flash("Selecione pelo menos um acesso para o usuário comum.", "error")
            return redirect(url_for("usuarios"))
        try:
            with closing(get_conn()) as conn:
                conn.execute(
                    "INSERT INTO users (nome, username, password_hash, role, permission_level, access_rules, ativo, criado_em) VALUES (?, ?, ?, ?, ?, ?, 1, ?)",
                    (nome, username, generate_password_hash(password), role, permission_level, serialize_access_rules(access_rules), agora_iso()),
                )
                conn.commit()
            flash("Usuário criado com sucesso.", "success")
        except sqlite3.IntegrityError:
            flash("Esse login já existe.", "error")
        return redirect(url_for("usuarios"))

    users = query_all("SELECT * FROM users ORDER BY ativo DESC, CASE permission_level WHEN 'admin' THEN 0 ELSE 1 END, role, nome")
    return render_template(
        "usuarios.html",
        title="Usuários",
        users=users,
        access_options=ACCESS_OPTIONS,
    )


@app.post("/usuarios/<int:user_id>/salvar")
@login_required
@module_required("usuarios")
@roles_required("admin")
def salvar_usuario(user_id: int) -> Response:
    nome = request.form.get("nome", "").strip()
    username = request.form.get("username", "").strip()
    role = normalize_role(request.form.get("role", "separador"))
    permission_level = normalize_permission_level(request.form.get("permission_level", "comum"), role)
    nova_senha = request.form.get("nova_senha", "")
    access_rules = set(request.form.getlist("access_rules"))
    if permission_level == "admin":
        access_rules = set(ACCESS_KEYS)
    else:
        access_rules = {item for item in access_rules if item in ACCESS_KEYS}

    if not nome or not username:
        flash("Preencha nome e login do usuário.", "error")
        return redirect(url_for("usuarios"))
    if permission_level != "admin" and not access_rules:
        flash("Selecione pelo menos um acesso para o usuário comum.", "error")
        return redirect(url_for("usuarios"))
    if nova_senha and len(nova_senha) < 4:
        flash("A nova senha precisa ter pelo menos 4 caracteres.", "error")
        return redirect(url_for("usuarios"))

    with closing(get_conn()) as conn:
        user = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        if user is None:
            flash("Usuário não encontrado.", "error")
            return redirect(url_for("usuarios"))

        if user_id == g.user["id"] and not user_is_admin({**dict(user), "role": role, "permission_level": permission_level}):
            if count_admin_users(conn, exclude_user_id=user_id) == 0:
                flash("Não é possível remover o nível admin do último admin ativo.", "error")
                return redirect(url_for("usuarios"))

        try:
            conn.execute(
                "UPDATE users SET nome = ?, username = ?, role = ?, permission_level = ?, access_rules = ? WHERE id = ?",
                (nome, username, role, permission_level, serialize_access_rules(access_rules), user_id),
            )
            if nova_senha:
                conn.execute(
                    "UPDATE users SET password_hash = ? WHERE id = ?",
                    (generate_password_hash(nova_senha), user_id),
                )
            conn.commit()
        except sqlite3.IntegrityError:
            flash("Esse login já existe.", "error")
            return redirect(url_for("usuarios"))

    flash("Usuário atualizado com sucesso.", "success")
    return redirect(url_for("usuarios"))


@app.post("/usuarios/<int:user_id>/alternar")
@login_required
@module_required("usuarios")
@roles_required("admin")
def alternar_usuario(user_id: int) -> Response:
    if user_id == g.user["id"]:
        flash("Você não pode desativar seu próprio usuário por aqui.", "error")
        return redirect(url_for("usuarios"))
    with closing(get_conn()) as conn:
        row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        if row is None:
            flash("Usuário não encontrado.", "error")
            return redirect(url_for("usuarios"))
        novo = 0 if row["ativo"] else 1
        if novo == 0 and user_is_admin(row) and count_admin_users(conn, exclude_user_id=user_id) == 0:
            flash("Não é possível desativar o último admin ativo do sistema.", "error")
            return redirect(url_for("usuarios"))
        conn.execute("UPDATE users SET ativo = ? WHERE id = ?", (novo, user_id))
        conn.commit()
    flash("Usuário atualizado.", "success")
    return redirect(url_for("usuarios"))


def usuario_tem_vinculos(conn: sqlite3.Connection, user_id: int) -> bool:
    counts = [
        conn.execute("SELECT COUNT(*) AS c FROM separations WHERE responsavel_id = ? OR conferente_id = ? OR criado_por = ?", (user_id, user_id, user_id)).fetchone()["c"],
        conn.execute("SELECT COUNT(*) AS c FROM stock_movements WHERE criado_por = ?", (user_id,)).fetchone()["c"],
    ]
    return any(int(c or 0) > 0 for c in counts)


@app.post("/usuarios/<int:user_id>/excluir")
@login_required
@module_required("usuarios")
@roles_required("admin")
def excluir_usuario(user_id: int) -> Response:
    if user_id == g.user["id"]:
        flash("Você não pode excluir o próprio usuário logado.", "error")
        return redirect(url_for("usuarios"))

    with closing(get_conn()) as conn:
        user = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        if user is None:
            flash("Usuário não encontrado.", "error")
            return redirect(url_for("usuarios"))

        outros_admins = count_admin_users(conn, exclude_user_id=user_id)
        if user_is_admin(user) and int(outros_admins or 0) == 0:
            flash("Não é possível excluir o último admin do sistema.", "error")
            return redirect(url_for("usuarios"))

        if usuario_tem_vinculos(conn, user_id):
            flash("Esse usuário já tem vínculo com separações ou movimentações. Desative em vez de excluir.", "error")
            return redirect(url_for("usuarios"))

        conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
        conn.commit()

    flash("Usuário excluído com sucesso.", "success")
    return redirect(url_for("usuarios"))


@app.route("/lojas", methods=["GET", "POST"])
@login_required
@module_required("lojas")
@roles_required("admin")
def lojas() -> str | Response:
    if request.method == "POST":
        nome = request.form.get("nome", "").strip()
        if not nome:
            flash("Informe o nome da loja.", "error")
            return redirect(url_for("lojas"))
        try:
            with closing(get_conn()) as conn:
                conn.execute(
                    "INSERT INTO stores (nome, ativo, criado_em) VALUES (?, 1, ?)",
                    (nome, agora_iso()),
                )
                conn.commit()
            flash("Loja cadastrada com sucesso.", "success")
        except sqlite3.IntegrityError:
            flash("Essa loja já existe.", "error")
        return redirect(url_for("lojas"))

    stores = query_all("SELECT * FROM stores ORDER BY ativo DESC, nome")
    return render_template("lojas.html", title="Lojas", stores=stores)


@app.post("/lojas/<int:store_id>/alternar")
@login_required
@module_required("lojas")
@roles_required("admin")
def alternar_loja(store_id: int) -> Response:
    with closing(get_conn()) as conn:
        row = conn.execute("SELECT ativo FROM stores WHERE id = ?", (store_id,)).fetchone()
        if row is None:
            flash("Loja não encontrada.", "error")
            return redirect(url_for("lojas"))
        conn.execute("UPDATE stores SET ativo = ? WHERE id = ?", (0 if row["ativo"] else 1, store_id))
        conn.commit()
    flash("Loja atualizada.", "success")
    return redirect(url_for("lojas"))


@app.post("/lojas/<int:store_id>/excluir")
@login_required
@module_required("lojas")
@roles_required("admin")
def excluir_loja(store_id: int) -> Response:
    with closing(get_conn()) as conn:
        loja = conn.execute("SELECT * FROM stores WHERE id = ?", (store_id,)).fetchone()
        if loja is None:
            flash("Loja não encontrada.", "error")
            return redirect(url_for("lojas"))

        usos = conn.execute("SELECT COUNT(*) AS c FROM separations WHERE store_id = ?", (store_id,)).fetchone()["c"]
        if int(usos or 0) > 0:
            flash("Essa loja já foi usada em separações. Desative em vez de excluir.", "error")
            return redirect(url_for("lojas"))

        conn.execute("DELETE FROM stores WHERE id = ?", (store_id,))
        conn.commit()

    flash("Loja excluída com sucesso.", "success")
    return redirect(url_for("lojas"))


@app.route("/configuracoes", methods=["GET", "POST"])
@login_required
@module_required("configuracoes")
@roles_required("admin")
def configuracoes() -> str | Response:
    if request.method == "POST":
        set_setting("vincular_estoque", "1" if request.form.get("vincular_estoque") == "1" else "0")
        set_setting("usar_conferente", "1" if request.form.get("usar_conferente") == "1" else "0")
        flash("Configuração salva com sucesso.", "success")
        return redirect(url_for("configuracoes"))
    return render_template(
        "configuracoes.html",
        title="Configurações",
        vincular_estoque=get_setting("vincular_estoque", "1") == "1",
        usar_conferente=get_setting("usar_conferente", "1") == "1",
    )




@app.route("/estoque", methods=["GET", "POST"])
@login_required
@module_required("estoque")
def estoque() -> str | Response:
    if request.method == "POST":
        if not can_edit_stock_registration(g.user):
            return forbidden_redirect("Somente usuários com permissão de admin podem cadastrar ou editar itens do estoque.")

        codigo = request.form.get("codigo", "").strip()
        descricao = request.form.get("descricao", "").strip()
        codigo_barras = request.form.get("codigo_barras", "").strip()
        try:
            fator_embalagem = parse_fator_embalagem(request.form.get("fator_embalagem", "1"))
            quantidade = parse_float(request.form.get("quantidade_atual", ""), "Quantidade")
            custo = parse_float(request.form.get("custo_unitario", "0") or "0", "Custo unitário")
        except ValueError as exc:
            flash(str(exc), "error")
            return redirect(url_for("estoque"))

        if not codigo or not descricao:
            flash("Informe código e descrição.", "error")
            return redirect(url_for("estoque"))

        redirect_q = request.form.get("redirect_q", "").strip()
        redirect_somente = "1" if request.form.get("redirect_somente_com_saldo") == "1" else "0"

        with closing(get_conn()) as conn:
            existente = conn.execute(
                "SELECT * FROM stock_items WHERE codigo = ? OR (codigo_barras IS NOT NULL AND codigo_barras <> '' AND codigo_barras = ?)",
                (codigo, codigo),
            ).fetchone()
            if existente:
                delta = quantidade - float(existente["quantidade_atual"])
                conn.execute(
                    "UPDATE stock_items SET codigo = ?, codigo_barras = ?, descricao = ?, fator_embalagem = ?, quantidade_atual = ?, custo_unitario = ?, ativo = 1, atualizado_em = ? WHERE id = ?",
                    (codigo, codigo_barras, descricao, fator_embalagem, quantidade, custo, agora_iso(), existente["id"]),
                )
                conn.execute(
                    "INSERT INTO stock_movements (stock_item_id, tipo, quantidade, observacao, referencia_tipo, referencia_id, criado_por, criado_em) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (existente["id"], "AJUSTE_MANUAL", delta, "Ajuste manual do cadastro", "ESTOQUE", existente["id"], g.user["id"], agora_iso()),
                )
                flash("Produto atualizado no estoque.", "success")
            else:
                cursor = conn.execute(
                    "INSERT INTO stock_items (codigo, codigo_barras, descricao, fator_embalagem, quantidade_atual, custo_unitario, ativo, atualizado_em) VALUES (?, ?, ?, ?, ?, ?, 1, ?)",
                    (codigo, codigo_barras, descricao, fator_embalagem, quantidade, custo, agora_iso()),
                )
                conn.execute(
                    "INSERT INTO stock_movements (stock_item_id, tipo, quantidade, observacao, referencia_tipo, referencia_id, criado_por, criado_em) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (cursor.lastrowid, "ENTRADA_INICIAL", quantidade, "Cadastro inicial do produto", "ESTOQUE", cursor.lastrowid, g.user["id"], agora_iso()),
                )
                flash("Produto cadastrado no estoque.", "success")
            conn.commit()
        return redirect(url_for("estoque", q=redirect_q, somente_com_saldo=redirect_somente))

    filters = sanitize_stock_history_filters(request.args)
    termo = filters["q"]
    somente_com_saldo = request.args.get("somente_com_saldo", "0") == "1"
    hist_usuario = filters["hist_usuario"]
    hist_tipo = filters["hist_tipo"]
    hist_data_inicial = filters["hist_data_inicial"]
    hist_data_final = filters["hist_data_final"]
    stock_items: list[sqlite3.Row] = []
    busca_realizada = bool(termo)

    if termo:
        filtros = ["ativo = 1"]
        params: list[Any] = []
        filtros.append("(codigo = ? OR codigo_barras = ? OR codigo LIKE ? OR descricao LIKE ? OR codigo_barras LIKE ?)")
        like = f"%{termo}%"
        params.extend([termo, termo, like, like, like])
        if somente_com_saldo:
            filtros.append("quantidade_atual > 0")
        where_sql = " AND ".join(filtros)
        stock_items = query_all(
            f"""
            SELECT *
            FROM stock_items
            WHERE {where_sql}
            ORDER BY
                CASE
                    WHEN codigo = ? THEN 0
                    WHEN codigo_barras = ? THEN 1
                    WHEN codigo LIKE ? THEN 2
                    ELSE 3
                END,
                descricao COLLATE NOCASE ASC,
                codigo ASC
            LIMIT 80
            """,
            params + [termo, termo, like],
        )

    stock_movements = fetch_stock_movements(filters, limit=80)
    history_user_options = query_all(
        "SELECT id, nome, username FROM users WHERE ativo = 1 ORDER BY nome COLLATE NOCASE ASC, username COLLATE NOCASE ASC"
    )

    return render_template(
        "estoque.html",
        title="Estoque",
        stock_items=stock_items,
        stock_movements=stock_movements,
        termo_busca=termo,
        somente_com_saldo=somente_com_saldo,
        busca_realizada=busca_realizada,
        hist_usuario=hist_usuario,
        hist_tipo=hist_tipo,
        hist_data_inicial=hist_data_inicial,
        hist_data_final=hist_data_final,
        history_user_options=history_user_options,
        stock_movement_type_options=STOCK_MOVEMENT_TYPE_OPTIONS,
    )


@app.post("/estoque/<int:stock_item_id>/editar")
@login_required
@module_required("estoque")
def editar_item_estoque(stock_item_id: int) -> Response:
    if not can_edit_stock_registration(g.user):
        return forbidden_redirect("Somente usuários com permissão de admin podem editar embalagem ou valor do estoque.")

    redirect_q = request.form.get("redirect_q", "").strip()
    redirect_somente = "1" if request.form.get("redirect_somente_com_saldo") == "1" else "0"

    try:
        fator_embalagem = parse_fator_embalagem(request.form.get("fator_embalagem", "1"))
        custo = parse_float(request.form.get("custo_unitario", "0") or "0", "Custo unitário")
    except ValueError as exc:
        flash(str(exc), "error")
        return redirect(url_for("estoque", q=redirect_q, somente_com_saldo=redirect_somente))

    with closing(get_conn()) as conn:
        item = conn.execute("SELECT * FROM stock_items WHERE id = ?", (stock_item_id,)).fetchone()
        if item is None:
            flash("Produto não encontrado.", "error")
            return redirect(url_for("estoque", q=redirect_q, somente_com_saldo=redirect_somente))

        conn.execute(
            "UPDATE stock_items SET fator_embalagem = ?, custo_unitario = ?, atualizado_em = ? WHERE id = ?",
            (fator_embalagem, custo, agora_iso(), stock_item_id),
        )
        conn.commit()

    flash("Embalagem e valor atualizados.", "success")
    return redirect(url_for("estoque", q=redirect_q, somente_com_saldo=redirect_somente))


@app.post("/estoque/<int:stock_item_id>/ajustar")
@login_required
@module_required("estoque")
def ajustar_estoque(stock_item_id: int) -> Response:
    if not can_adjust_stock(g.user):
        return forbidden_redirect("Sem permissão para ajustar estoque.")

    redirect_q = request.form.get("redirect_q", "").strip()
    redirect_somente = "1" if request.form.get("redirect_somente_com_saldo") == "1" else "0"
    try:
        nova_qtd = parse_float(request.form.get("nova_quantidade", ""), "Quantidade")
    except ValueError as exc:
        flash(str(exc), "error")
        return redirect(url_for("estoque", q=redirect_q, somente_com_saldo=redirect_somente))

    with closing(get_conn()) as conn:
        item = conn.execute("SELECT * FROM stock_items WHERE id = ?", (stock_item_id,)).fetchone()
        if item is None:
            flash("Produto não encontrado.", "error")
            return redirect(url_for("estoque", q=redirect_q, somente_com_saldo=redirect_somente))
        delta = nova_qtd - float(item["quantidade_atual"])
        conn.execute(
            "UPDATE stock_items SET quantidade_atual = ?, atualizado_em = ? WHERE id = ?",
            (nova_qtd, agora_iso(), stock_item_id),
        )
        conn.execute(
            "INSERT INTO stock_movements (stock_item_id, tipo, quantidade, observacao, referencia_tipo, referencia_id, criado_por, criado_em) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (stock_item_id, "RECONTAGEM", delta, "Recontagem manual do estoque", "ESTOQUE", stock_item_id, g.user["id"], agora_iso()),
        )
        conn.commit()

    flash("Estoque ajustado.", "success")
    return redirect(url_for("estoque", q=redirect_q, somente_com_saldo=redirect_somente))


@app.post("/estoque/<int:stock_item_id>/remover")
@login_required
@module_required("estoque")
def remover_item_estoque(stock_item_id: int) -> Response:
    if not user_is_admin(g.user):
        flash("Apenas o admin pode remover item do estoque.", "error")
        return redirect(url_for("estoque"))

    redirect_q = request.form.get("redirect_q", "").strip()
    redirect_somente = "1" if request.form.get("redirect_somente_com_saldo") == "1" else "0"

    with closing(get_conn()) as conn:
        item = conn.execute("SELECT * FROM stock_items WHERE id = ?", (stock_item_id,)).fetchone()
        if item is None or int(item["ativo"] or 0) != 1:
            flash("Item de estoque não encontrado.", "error")
            return redirect(url_for("estoque", q=redirect_q, somente_com_saldo=redirect_somente))

        quantidade_atual = float(item["quantidade_atual"] or 0)
        conn.execute(
            "UPDATE stock_items SET ativo = 0, quantidade_atual = 0, atualizado_em = ? WHERE id = ?",
            (agora_iso(), stock_item_id),
        )
        conn.execute(
            "INSERT INTO stock_movements (stock_item_id, tipo, quantidade, observacao, referencia_tipo, referencia_id, criado_por, criado_em) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                stock_item_id,
                "REMOVIDO_ESTOQUE",
                -quantidade_atual,
                "Item removido da lista ativa do estoque",
                "ESTOQUE",
                stock_item_id,
                g.user["id"],
                agora_iso(),
            ),
        )
        conn.commit()

    flash("Item removido do estoque.", "success")
    return redirect(url_for("estoque", q=redirect_q, somente_com_saldo=redirect_somente))



def usuarios_por_role(role: str | None = None) -> list[sqlite3.Row]:
    if role:
        return query_all("SELECT * FROM users WHERE ativo = 1 AND role = ? ORDER BY nome", (role,))
    return query_all("SELECT * FROM users WHERE ativo = 1 ORDER BY nome")


def copiar_pendencias_para_nova_separacao(conn: sqlite3.Connection, separation_id: int, store_id: int) -> int:
    pendencias = conn.execute(
        """
        SELECT si.*
        FROM separation_items si
        JOIN separations s ON s.id = si.separation_id
        WHERE s.store_id = ?
          AND s.status = 'FINALIZADA'
          AND COALESCE(si.carryover_copied, 0) = 0
          AND (COALESCE(si.quantidade_pedida, 0) - COALESCE(si.quantidade_separada, 0)) > 0
        ORDER BY COALESCE(s.finalizado_em, s.criado_em) ASC, si.id ASC
        """,
        (store_id,),
    ).fetchall()

    copiados = 0
    for item in pendencias:
        restante = float(item["quantidade_pedida"] or 0) - float(item["quantidade_separada"] or 0)
        if restante <= 0:
            continue

        existente = conn.execute(
            "SELECT id, quantidade_pedida FROM separation_items WHERE separation_id = ? AND codigo = ?",
            (separation_id, item["codigo"]),
        ).fetchone()
        if existente is None:
            conn.execute(
                """
                INSERT INTO separation_items (
                    separation_id, codigo, descricao, fator_embalagem, quantidade_pedida, quantidade_separada, status,
                    custo_unitario_ref, carryover_source_item_id, carryover_copied, criado_em, atualizado_em
                ) VALUES (?, ?, ?, ?, ?, 0, 'PENDENTE', ?, ?, 0, ?, ?)
                """,
                (
                    separation_id,
                    item["codigo"],
                    item["descricao"],
                    float(item["fator_embalagem"] or 1),
                    restante,
                    item["custo_unitario_ref"],
                    item["id"],
                    agora_iso(),
                    agora_iso(),
                ),
            )
        else:
            nova_quantidade = float(existente["quantidade_pedida"] or 0) + restante
            conn.execute(
                "UPDATE separation_items SET descricao = ?, fator_embalagem = ?, quantidade_pedida = ?, atualizado_em = ? WHERE id = ?",
                (item["descricao"], float(item["fator_embalagem"] or 1), nova_quantidade, agora_iso(), existente["id"]),
            )
        conn.execute(
            "UPDATE separation_items SET carryover_copied = 1, atualizado_em = ? WHERE id = ?",
            (agora_iso(), item["id"]),
        )
        copiados += 1

    return copiados


def desfazer_pendencias_transferidas(conn: sqlite3.Connection, separation_id: int) -> None:
    origem_ids = [
        row["carryover_source_item_id"]
        for row in conn.execute(
            "SELECT DISTINCT carryover_source_item_id FROM separation_items WHERE separation_id = ? AND carryover_source_item_id IS NOT NULL",
            (separation_id,),
        ).fetchall()
    ]
    if origem_ids:
        conn.execute(
            f"UPDATE separation_items SET carryover_copied = 0, atualizado_em = ? WHERE id IN ({','.join('?' for _ in origem_ids)})",
            (agora_iso(), *origem_ids),
        )


@app.route("/separacoes/nova", methods=["GET", "POST"])
@login_required
@module_required("pedidos")
@roles_required("admin")
def nova_separacao() -> str | Response:
    if request.method == "POST":
        lote_nome = request.form.get("lote_nome", "").strip()
        data_referencia = request.form.get("data_referencia", "").strip()
        responsavel_id = request.form.get("responsavel_id", "").strip()
        usar_conferente = get_setting("usar_conferente", "1") == "1"
        conferente_id = request.form.get("conferente_id", "").strip() or None
        if not usar_conferente:
            conferente_id = None
        stores = request.form.getlist("stores")
        usar_estoque = 1 if request.form.get("usar_estoque") == "1" else 0
        trazer_pendencias = request.form.get("trazer_pendencias") == "1"
        observacao = request.form.get("observacao", "").strip()

        if not lote_nome or not data_referencia or not responsavel_id.isdigit() or not stores:
            flash("Preencha lote, data, responsável e selecione ao menos uma loja.", "error")
            return redirect(url_for("nova_separacao"))

        lote_codigo = novo_lote_codigo()
        pendencias_copiadas = 0
        with closing(get_conn()) as conn:
            for store_id in stores:
                if not str(store_id).isdigit():
                    continue
                cursor = conn.execute(
                    """
                    INSERT INTO separations (
                        lote_codigo, lote_nome, data_referencia, store_id, responsavel_id, conferente_id,
                        status, usar_estoque, observacao, criado_por, criado_em
                    ) VALUES (?, ?, ?, ?, ?, ?, 'ABERTA', ?, ?, ?, ?)
                    """,
                    (
                        lote_codigo,
                        lote_nome,
                        data_referencia,
                        int(store_id),
                        int(responsavel_id),
                        int(conferente_id) if conferente_id and str(conferente_id).isdigit() else None,
                        usar_estoque,
                        observacao,
                        g.user["id"],
                        agora_iso(),
                    ),
                )
                if trazer_pendencias:
                    pendencias_copiadas += copiar_pendencias_para_nova_separacao(conn, cursor.lastrowid, int(store_id))
            conn.commit()

        mensagem = "Separações criadas. Agora você pode lançar os itens do lote em uma tela única, com quantidade diferente para cada loja, sem entrar uma por uma."
        if trazer_pendencias and pendencias_copiadas:
            mensagem += f" Também trouxe {pendencias_copiadas} pendência(s) parcial(is) de dias anteriores para completar o restante."
        flash(mensagem, "success")
        return redirect(url_for("grade_lote", lote_codigo=lote_codigo))

    return render_template(
        "nova_separacao.html",
        title="Criar separações",
        hoje=datetime.now().strftime("%Y-%m-%d"),
        stores=query_all("SELECT * FROM stores WHERE ativo = 1 ORDER BY nome"),
        separadores=usuarios_por_role("separador"),
        conferentes=usuarios_por_role("conferente"),
        usar_conferente=get_setting("usar_conferente", "1") == "1",
        trazer_pendencias_padrao=True,
    )


def listar_lotes_em_aberto() -> list[sqlite3.Row]:
    chave_expr = lote_operacao_chave_expr("s")
    return query_all(
        f"""
        SELECT {chave_expr} AS operacao_chave,
               MAX(s.lote_codigo) AS lote_codigo,
               s.lote_nome,
               s.data_referencia,
               MAX(r.nome) AS responsavel_nome,
               MAX(c.nome) AS conferente_nome,
               COUNT(*) AS total_lojas,
               SUM(CASE WHEN s.status = 'FINALIZADA' THEN 1 ELSE 0 END) AS lojas_finalizadas,
               GROUP_CONCAT(st.nome, ' • ') AS lojas
        FROM separations s
        JOIN stores st ON st.id = s.store_id
        LEFT JOIN users r ON r.id = s.responsavel_id
        LEFT JOIN users c ON c.id = s.conferente_id
        GROUP BY operacao_chave, s.lote_nome, s.data_referencia
        HAVING SUM(CASE WHEN s.status NOT IN ('FINALIZADA', 'CANCELADA') THEN 1 ELSE 0 END) > 0
        ORDER BY MAX(s.id) DESC
        """
    )


def carregar_lote(operacao_chave: str) -> list[sqlite3.Row]:
    chave_expr = lote_operacao_chave_expr("s")
    rows = query_all(
        f"""
        SELECT s.*, st.nome AS store_nome,
               r.nome AS responsavel_nome,
               c.nome AS conferente_nome,
               {chave_expr} AS operacao_chave
        FROM separations s
        JOIN stores st ON st.id = s.store_id
        LEFT JOIN users r ON r.id = s.responsavel_id
        LEFT JOIN users c ON c.id = s.conferente_id
        WHERE {chave_expr} = ?
          AND s.status <> 'CANCELADA'
        ORDER BY s.id ASC
        """,
        (operacao_chave,),
    )
    return sort_store_rows(rows)


def produtos_do_lote(operacao_chave: str, separacoes: list[sqlite3.Row]) -> list[dict[str, Any]]:
    store_ids = [row["store_id"] for row in separacoes]
    store_names = {row["store_id"]: row["store_nome"] for row in separacoes}
    chave_expr = lote_operacao_chave_expr("s")
    rows = query_all(
        f"""
        SELECT si.codigo, si.descricao, si.fator_embalagem, si.quantidade_pedida, s.store_id
        FROM separation_items si
        JOIN separations s ON s.id = si.separation_id
        WHERE {chave_expr} = ?
        ORDER BY si.descricao COLLATE NOCASE ASC, si.codigo ASC, s.store_id ASC
        """
        ,
        (operacao_chave,),
    )
    produtos: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = (row["codigo"], row["descricao"])
        if key not in produtos:
            produtos[key] = {
                "codigo": row["codigo"],
                "descricao": row["descricao"],
                "fator_embalagem": float(row["fator_embalagem"] or 1),
                "total": 0.0,
                "quantidades": {},
            }
        quantidade = float(row["quantidade_pedida"] or 0)
        produtos[key]["total"] += quantidade
        produtos[key]["quantidades"][row["store_id"]] = quantidade

    resultado: list[dict[str, Any]] = []
    for produto in produtos.values():
        produto["linhas"] = [
            {
                "store_id": store_id,
                "store_nome": store_names[store_id],
                "quantidade": produto["quantidades"].get(store_id, 0),
            }
            for store_id in store_ids
        ]
        resultado.append(produto)
    return resultado





def lotes_visiveis_para_usuario(user: sqlite3.Row | None) -> list[sqlite3.Row]:
    todos = listar_lotes_em_aberto()
    if user is None or user_is_admin(user):
        return todos
    resultado: list[sqlite3.Row] = []
    for lote in todos:
        separacoes = carregar_lote(lote["operacao_chave"])
        if normalize_role(user["role"]) == "separador" and any(sep["responsavel_id"] == user["id"] for sep in separacoes):
            resultado.append(lote)
        elif normalize_role(user["role"]) == "conferente" and any(sep["conferente_id"] == user["id"] for sep in separacoes):
            resultado.append(lote)
    return resultado


def pode_acessar_lote_operacao(separacoes: list[sqlite3.Row], modo: str) -> bool:
    if g.user is None:
        return False
    if user_is_admin(g.user):
        return True
    if modo == "separacao":
        return normalize_role(g.user["role"]) == "separador" and any(sep["responsavel_id"] == g.user["id"] for sep in separacoes)
    if modo == "conferencia":
        return normalize_role(g.user["role"]) == "conferente" and any(sep["conferente_id"] == g.user["id"] for sep in separacoes)
    return False


def itens_do_lote_para_fluxo(operacao_chave: str, separacoes: list[sqlite3.Row]) -> list[dict[str, Any]]:
    store_ids = [row["store_id"] for row in separacoes]
    store_names = {row["store_id"]: row["store_nome"] for row in separacoes}
    separation_ids = {row["store_id"]: row["id"] for row in separacoes}
    chave_expr = lote_operacao_chave_expr("s")
    rows = query_all(
        f"""
        SELECT si.id, si.codigo, si.descricao, si.fator_embalagem, si.quantidade_pedida, si.quantidade_separada,
               COALESCE(si.quantidade_conferida, si.quantidade_separada) AS quantidade_conferencia_visivel,
               si.status, s.id AS separation_id, s.store_id
        FROM separation_items si
        JOIN separations s ON s.id = si.separation_id
        WHERE {chave_expr} = ?
        ORDER BY si.descricao COLLATE NOCASE ASC, si.codigo ASC, s.store_id ASC
        """,
        (operacao_chave,),
    )
    produtos: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = (row["codigo"], row["descricao"])
        if key not in produtos:
            produtos[key] = {
                "codigo": row["codigo"],
                "descricao": row["descricao"],
                "fator_embalagem": float(row["fator_embalagem"] or 1),
                "total_pedido": 0.0,
                "total_separado": 0.0,
                "total_conferido": 0.0,
                "stores": {},
            }
        pedido = float(row["quantidade_pedida"] or 0)
        separado = float(row["quantidade_separada"] or 0)
        conferido = float(row["quantidade_conferencia_visivel"] or 0)
        produtos[key]["total_pedido"] += pedido
        produtos[key]["total_separado"] += separado
        produtos[key]["total_conferido"] += conferido
        produtos[key]["stores"][row["store_id"]] = {
            "item_id": row["id"],
            "separation_id": row["separation_id"],
            "store_id": row["store_id"],
            "store_nome": store_names[row["store_id"]],
            "quantidade_pedida": pedido,
            "quantidade_separada": separado,
            "quantidade_conferida": conferido,
            "fator_embalagem": float(row["fator_embalagem"] or 1),
            "status": row["status"],
        }

    resultado: list[dict[str, Any]] = []
    for produto in produtos.values():
        linhas = []
        for store_id in store_ids:
            linha = produto["stores"].get(store_id)
            if linha is None:
                linha = {
                    "item_id": None,
                    "separation_id": separation_ids[store_id],
                    "store_id": store_id,
                    "store_nome": store_names[store_id],
                    "quantidade_pedida": 0.0,
                    "quantidade_separada": 0.0,
                    "quantidade_conferida": 0.0,
                    "fator_embalagem": float(produto["fator_embalagem"] or 1),
                    "status": "PENDENTE",
                }
            linhas.append(linha)
        produto["linhas"] = linhas
        resultado.append(produto)
    return resultado


def distribuir_quantidades_racionalizadas(total_real: float, linhas: list[dict[str, Any]], lojas_fixas: set[int]) -> dict[int, float]:
    pedido_por_loja = {linha["store_id"]: float(linha["quantidade_pedida"] or 0) for linha in linhas}
    distribuicao = {store_id: 0.0 for store_id in pedido_por_loja}
    total_fixo = 0.0
    for store_id in lojas_fixas:
        valor = min(pedido_por_loja.get(store_id, 0.0), max(total_real, 0.0))
        distribuicao[store_id] = valor
        total_fixo += valor
    restante = max(total_real - total_fixo, 0.0)
    livres = [store_id for store_id in pedido_por_loja if store_id not in lojas_fixas]
    total_pedido_livre = sum(pedido_por_loja[store_id] for store_id in livres)
    if total_pedido_livre <= 0:
        return distribuicao
    base = {}
    for store_id in livres:
        exato = restante * (pedido_por_loja[store_id] / total_pedido_livre)
        base[store_id] = exato
        distribuicao[store_id] = float(int(exato))
    usado = sum(distribuicao.values())
    sobra = int(round(total_real - usado))
    ordem = sorted(livres, key=lambda sid: (-(base[sid] - int(base[sid])), natural_store_sort_key(str(sid))))
    idx = 0
    while sobra > 0 and ordem:
        store_id = ordem[idx % len(ordem)]
        if distribuicao[store_id] < pedido_por_loja[store_id]:
            distribuicao[store_id] += 1
            sobra -= 1
        idx += 1
        if idx > 10000:
            break
    for store_id, pedido in pedido_por_loja.items():
        if distribuicao[store_id] > pedido:
            distribuicao[store_id] = pedido
    return distribuicao


def atualizar_status_item(qtd_pedida: float, qtd_real: float, conferido: bool = False) -> str:
    if qtd_real <= 0:
        return "PENDENTE"
    if qtd_real < qtd_pedida:
        return "CONFERIDO" if conferido else "PARCIAL"
    return "CONFERIDO" if conferido else "SEPARADO"


def itens_pendentes_lote(operacao_chave: str, modo: str) -> list[dict[str, str]]:
    produtos = itens_do_lote_para_fluxo(operacao_chave, carregar_lote(operacao_chave))
    resultado: list[dict[str, str]] = []
    for produto in produtos:
        if modo == "separacao":
            pendente = any(float(linha["quantidade_separada"] or 0) < float(linha["quantidade_pedida"] or 0) for linha in produto["linhas"])
        else:
            pendente = any(str(linha["status"]) != "CONFERIDO" and float(linha["quantidade_separada"] or 0) > 0 for linha in produto["linhas"])
        if pendente or not resultado:
            resultado.append({"codigo": produto["codigo"], "descricao": produto["descricao"]})
    return resultado

def separation_visibility_clause() -> tuple[str, list[Any]]:
    user = g.user
    if user_is_admin(user):
        return "WHERE s.status <> 'CANCELADA'", []
    if normalize_role(user["role"]) == "separador":
        return "WHERE s.status <> 'CANCELADA' AND s.responsavel_id = ?", [user["id"]]
    return "WHERE s.status <> 'CANCELADA' AND (s.conferente_id = ? OR s.responsavel_id = ?)", [user["id"], user["id"]]




@app.get("/lotes")
@login_required
@module_required("lotes")
@roles_required("admin")
def listar_lotes() -> str:
    return render_template("lotes.html", title="Lotes", lotes=listar_lotes_em_aberto())




@app.route("/lotes/<lote_codigo>/grade", methods=["GET", "POST"])
@login_required
@module_required("lotes")
@roles_required("admin")
def grade_lote(lote_codigo: str) -> str | Response:
    separacoes = carregar_lote(lote_codigo)
    if not separacoes:
        flash("Lote não encontrado.", "error")
        return redirect(url_for("listar_lotes"))

    if request.method == "POST":
        codigo = request.form.get("codigo", "").strip()
        descricao = request.form.get("descricao", "").strip()
        try:
            fator_embalagem = parse_fator_embalagem(request.form.get("fator_embalagem", "1"))
        except ValueError as exc:
            flash(str(exc), "error")
            return redirect(url_for("grade_lote", lote_codigo=lote_codigo))
        if not codigo or not descricao:
            flash("Informe código e descrição.", "error")
            return redirect(url_for("grade_lote", lote_codigo=lote_codigo))

        quantidades: list[tuple[int, float, float]] = []
        for sep in separacoes:
            if sep["status"] == "FINALIZADA":
                continue
            raw = request.form.get(f"qty_{sep['id']}", "").strip()
            if not raw:
                continue
            try:
                quantidade_emb = parse_float(raw, f"Quantidade da loja {sep['store_nome']}")
            except ValueError as exc:
                flash(str(exc), "error")
                return redirect(url_for("grade_lote", lote_codigo=lote_codigo))
            if quantidade_emb > 0:
                quantidades.append((sep["id"], quantidade_emb, quantidade_emb * fator_embalagem))

        if not quantidades:
            flash("Preencha ao menos uma loja com quantidade maior que zero.", "error")
            return redirect(url_for("grade_lote", lote_codigo=lote_codigo))

        stock = query_one("SELECT custo_unitario FROM stock_items WHERE codigo = ?", (codigo,))
        custo_ref = stock["custo_unitario"] if stock else 0

        with closing(get_conn()) as conn:
            for separation_id, quantidade_emb, quantidade in quantidades:
                existente = conn.execute(
                    "SELECT id, quantidade_pedida FROM separation_items WHERE separation_id = ? AND codigo = ?",
                    (separation_id, codigo),
                ).fetchone()
                if existente is None:
                    conn.execute(
                        """
                        INSERT INTO separation_items (separation_id, codigo, descricao, fator_embalagem, quantidade_pedida, quantidade_separada, status, custo_unitario_ref, criado_em, atualizado_em)
                        VALUES (?, ?, ?, ?, ?, 0, 'PENDENTE', ?, ?, ?)
                        """,
                        (separation_id, codigo, descricao, fator_embalagem, quantidade, custo_ref, agora_iso(), agora_iso()),
                    )
                else:
                    nova_quantidade = float(existente["quantidade_pedida"]) + quantidade
                    conn.execute(
                        "UPDATE separation_items SET descricao = ?, fator_embalagem = ?, quantidade_pedida = ?, atualizado_em = ? WHERE id = ?",
                        (descricao, fator_embalagem, nova_quantidade, agora_iso(), existente["id"]),
                    )
            conn.commit()

        flash(f"Produto lançado em {len(quantidades)} loja(s) com quantidades individuais.", "success")
        return redirect(url_for("grade_lote", lote_codigo=lote_codigo))

    primeira = separacoes[0]
    return render_template(
        "grade_lote.html",
        title="Lançamento por lote",
        lote_codigo=lote_codigo,
        lote_nome=primeira["lote_nome"],
        data_referencia=primeira["data_referencia"],
        responsavel_nome=primeira["responsavel_nome"],
        conferente_nome=primeira["conferente_nome"],
        separacoes=separacoes,
        produtos=produtos_do_lote(lote_codigo, separacoes),
    )







@app.route("/lotes/<lote_codigo>/separar-itens", methods=["GET", "POST"])
@login_required
@module_required("separacoes")
def separar_itens_lote(lote_codigo: str) -> str | Response:
    separacoes = carregar_lote(lote_codigo)
    if not separacoes or not pode_acessar_lote_operacao(separacoes, "separacao"):
        flash("Lote não encontrado ou sem permissão para separar.", "error")
        return redirect(url_for("listar_separacoes"))

    produtos = itens_do_lote_para_fluxo(lote_codigo, separacoes)
    if not produtos:
        flash("Esse lote ainda não possui itens para separar.", "error")
        return redirect(url_for("grade_lote", lote_codigo=lote_codigo) if user_is_admin(g.user) else url_for("listar_separacoes"))

    codigo_atual = request.values.get("codigo", "").strip()
    produto_atual = next((p for p in produtos if p["codigo"] == codigo_atual), None) if codigo_atual else None
    if produto_atual is None:
        pendentes = itens_pendentes_lote(lote_codigo, "separacao")
        if pendentes:
            codigo_atual = pendentes[0]["codigo"]
            produto_atual = next((p for p in produtos if p["codigo"] == codigo_atual), produtos[0])
        else:
            produto_atual = produtos[0]
            codigo_atual = produto_atual["codigo"]

    if request.method == "POST":
        try:
            quantidade_real = parse_float(request.form.get("quantidade_real", "0"), "Quantidade real")
        except ValueError as exc:
            flash(str(exc), "error")
            return redirect(url_for("separar_itens_lote", lote_codigo=lote_codigo, codigo=codigo_atual))

        linhas = produto_atual["linhas"]
        if request.form.get("aplicar_racionalizacao") == "1":
            fixas: set[int] = set()
            for linha in linhas:
                if request.form.get(f"fixo_{linha['store_id']}") == "1":
                    fixas.add(int(linha["store_id"]))
            sugestao = distribuir_quantidades_racionalizadas(quantidade_real, linhas, fixas)
        else:
            sugestao = {}

        valores_salvar: dict[int, float] = {}
        for linha in linhas:
            campo = request.form.get(f"quantidade_loja_{linha['store_id']}", "").strip()
            if campo:
                try:
                    valores_salvar[int(linha["store_id"])] = parse_float(campo, f"Quantidade da {linha['store_nome']}")
                except ValueError as exc:
                    flash(str(exc), "error")
                    return redirect(url_for("separar_itens_lote", lote_codigo=lote_codigo, codigo=codigo_atual))
            else:
                valores_salvar[int(linha["store_id"])] = float(sugestao.get(int(linha["store_id"]), linha["quantidade_separada"] or 0))

        with closing(get_conn()) as conn:
            for linha in linhas:
                item_id = linha["item_id"]
                if not item_id:
                    continue
                qtd = float(valores_salvar.get(int(linha["store_id"]), 0))
                status = atualizar_status_item(float(linha["quantidade_pedida"] or 0), qtd, conferido=False)
                conn.execute(
                    "UPDATE separation_items SET quantidade_separada = ?, status = ?, atualizado_em = ?, quantidade_conferida = NULL, conferido_em = NULL WHERE id = ?",
                    (qtd, status, agora_iso(), item_id),
                )
                conn.execute(
                    "UPDATE separations SET status = CASE WHEN status = 'ABERTA' THEN 'EM_SEPARACAO' ELSE status END WHERE id = ? AND status <> 'FINALIZADA'",
                    (linha["separation_id"],),
                )
            conn.commit()

        produto_indices = [p["codigo"] for p in produtos]
        idx = produto_indices.index(codigo_atual)
        proximo_codigo = produto_indices[idx + 1] if idx + 1 < len(produto_indices) else codigo_atual
        flash("Separação do item salva com sucesso.", "success")
        return redirect(url_for("separar_itens_lote", lote_codigo=lote_codigo, codigo=proximo_codigo))

    lotes_visiveis = lotes_visiveis_para_usuario(g.user)
    return render_template(
        "operacao_item_lote.html",
        title="Separar itens do lote",
        modo="separacao",
        lotes=lotes_visiveis,
        lote_codigo=lote_codigo,
        separacoes=separacoes,
        produtos=produtos,
        produto_atual=produto_atual,
        responsavel_nome=separacoes[0]["responsavel_nome"],
        conferente_nome=separacoes[0]["conferente_nome"],
    )


@app.route("/lotes/<lote_codigo>/conferir-itens", methods=["GET", "POST"])
@login_required
@module_required("separacoes")
def conferir_itens_lote(lote_codigo: str) -> str | Response:
    separacoes = carregar_lote(lote_codigo)
    if not separacoes or not pode_acessar_lote_operacao(separacoes, "conferencia"):
        flash("Lote não encontrado ou sem permissão para conferir.", "error")
        return redirect(url_for("listar_separacoes"))

    produtos = itens_do_lote_para_fluxo(lote_codigo, separacoes)
    produtos_com_separacao = [p for p in produtos if any(float(l["quantidade_separada"] or 0) > 0 for l in p["linhas"]) ]
    if not produtos_com_separacao:
        flash("Esse lote ainda não possui itens separados para conferência.", "error")
        return redirect(url_for("listar_separacoes"))

    codigo_atual = request.values.get("codigo", "").strip()
    produto_atual = next((p for p in produtos_com_separacao if p["codigo"] == codigo_atual), None) if codigo_atual else None
    if produto_atual is None:
        pendentes = itens_pendentes_lote(lote_codigo, "conferencia")
        if pendentes:
            codigo_atual = pendentes[0]["codigo"]
            produto_atual = next((p for p in produtos_com_separacao if p["codigo"] == codigo_atual), produtos_com_separacao[0])
        else:
            produto_atual = produtos_com_separacao[0]
            codigo_atual = produto_atual["codigo"]

    if request.method == "POST":
        with closing(get_conn()) as conn:
            for linha in produto_atual["linhas"]:
                item_id = linha["item_id"]
                if not item_id:
                    continue
                try:
                    qtd_conf = parse_float(request.form.get(f"confirmada_loja_{linha['store_id']}", linha["quantidade_separada"]), f"Conferência da {linha['store_nome']}")
                except ValueError as exc:
                    flash(str(exc), "error")
                    return redirect(url_for("conferir_itens_lote", lote_codigo=lote_codigo, codigo=codigo_atual))
                status = atualizar_status_item(float(linha["quantidade_pedida"] or 0), qtd_conf, conferido=True)
                conn.execute(
                    "UPDATE separation_items SET quantidade_conferida = ?, quantidade_separada = ?, status = ?, conferido_em = ?, atualizado_em = ? WHERE id = ?",
                    (qtd_conf, qtd_conf, status, agora_iso(), agora_iso(), item_id),
                )
            for sep in separacoes:
                pend = conn.execute("SELECT COUNT(*) AS c FROM separation_items WHERE separation_id = ? AND status NOT IN ('CONFERIDO', 'PENDENTE')", (sep['id'],)).fetchone()["c"]
                tem_separado = conn.execute("SELECT COUNT(*) AS c FROM separation_items WHERE separation_id = ? AND quantidade_separada > 0", (sep['id'],)).fetchone()["c"]
                if tem_separado > 0 and pend == 0 and sep['status'] != 'FINALIZADA':
                    conn.execute("UPDATE separations SET status = 'AGUARDANDO_CONFERENCIA', enviado_conferencia_em = COALESCE(enviado_conferencia_em, ?) WHERE id = ?", (agora_iso(), sep['id']))
            conn.commit()
        codigos = [p["codigo"] for p in produtos_com_separacao]
        idx = codigos.index(codigo_atual)
        proximo_codigo = codigos[idx + 1] if idx + 1 < len(codigos) else codigo_atual
        flash("Conferência do item registrada com sucesso.", "success")
        return redirect(url_for("conferir_itens_lote", lote_codigo=lote_codigo, codigo=proximo_codigo))

    lotes_visiveis = lotes_visiveis_para_usuario(g.user)
    return render_template(
        "operacao_item_lote.html",
        title="Conferir itens do lote",
        modo="conferencia",
        lotes=lotes_visiveis,
        lote_codigo=lote_codigo,
        separacoes=separacoes,
        produtos=produtos_com_separacao,
        produto_atual=produto_atual,
        responsavel_nome=separacoes[0]["responsavel_nome"],
        conferente_nome=separacoes[0]["conferente_nome"],
    )


def finalizar_separacao_no_conn(conn: sqlite3.Connection, separation: sqlite3.Row, actor_id: int) -> float:
    itens = conn.execute("SELECT * FROM separation_items WHERE separation_id = ?", (separation['id'],)).fetchall()
    if not itens:
        return 0.0
    usar_controle_global = get_setting("vincular_estoque", "1") == "1"
    precisa_abater = usar_controle_global and bool(separation["usar_estoque"])
    if precisa_abater:
        problemas = validar_estoque_para_finalizacao(conn, separation['id'])
        if problemas:
            raise ValueError("Saldo insuficiente para finalizar com controle de estoque: " + "; ".join(problemas))
        for item in itens:
            stock = conn.execute("SELECT * FROM stock_items WHERE codigo = ?", (item['codigo'],)).fetchone()
            if stock is None:
                conn.execute(
                    "INSERT INTO stock_items (codigo, descricao, quantidade_atual, custo_unitario, atualizado_em) VALUES (?, ?, 0, ?, ?)",
                    (item['codigo'], item['descricao'], item['custo_unitario_ref'], agora_iso()),
                )
                stock = conn.execute("SELECT * FROM stock_items WHERE codigo = ?", (item['codigo'],)).fetchone()
            novo_saldo = float(stock['quantidade_atual']) - float(item['quantidade_separada'])
            conn.execute(
                "UPDATE stock_items SET quantidade_atual = ?, atualizado_em = ? WHERE id = ?",
                (novo_saldo, agora_iso(), stock['id']),
            )
            conn.execute(
                "INSERT INTO stock_movements (stock_item_id, tipo, quantidade, observacao, referencia_tipo, referencia_id, criado_por, criado_em) VALUES (?, 'SAIDA_SEPARACAO', ?, ?, 'SEPARACAO', ?, ?, ?)",
                (stock['id'], -float(item['quantidade_separada']), f"Saída da separação {separation['lote_nome']} - {separation['store_nome']}", separation['id'], actor_id, agora_iso()),
            )
    pendencias_restantes = sum(max(float(item['quantidade_pedida'] or 0) - float(item['quantidade_separada'] or 0), 0) for item in itens)
    conn.execute("UPDATE separations SET status = 'FINALIZADA', finalizado_em = ? WHERE id = ?", (agora_iso(), separation['id']))
    return pendencias_restantes


@app.post("/lotes/<lote_codigo>/finalizar-conferencia")
@login_required
@module_required("separacoes")
def finalizar_conferencia_lote(lote_codigo: str) -> Response:
    separacoes = carregar_lote(lote_codigo)
    if not separacoes or not pode_acessar_lote_operacao(separacoes, "conferencia"):
        flash("Lote não encontrado ou sem permissão para finalizar a conferência.", "error")
        return redirect(url_for("listar_separacoes"))
    try:
        pendencias = 0.0
        with closing(get_conn()) as conn:
            for sep in separacoes:
                if sep['status'] == 'FINALIZADA':
                    continue
                itens = conn.execute("SELECT COUNT(*) AS c FROM separation_items WHERE separation_id = ?", (sep['id'],)).fetchone()['c']
                if itens == 0:
                    continue
                pend_nao_conferidos = conn.execute("SELECT COUNT(*) AS c FROM separation_items WHERE separation_id = ? AND quantidade_separada > 0 AND status NOT IN ('CONFERIDO')", (sep['id'],)).fetchone()['c']
                if pend_nao_conferidos > 0:
                    raise ValueError(f"A loja {sep['store_nome']} ainda possui itens sem conferência.")
                pendencias += finalizar_separacao_no_conn(conn, sep, g.user['id'])
            conn.commit()
    except ValueError as exc:
        flash(str(exc), 'error')
        return redirect(url_for('conferir_itens_lote', lote_codigo=lote_codigo))
    if pendencias > 0:
        flash('Conferência concluída. O lote foi finalizado com pendências parciais registradas para reaproveitamento futuro.', 'success')
    else:
        flash('Conferência concluída e lote finalizado.', 'success')
    return redirect(url_for('listar_separacoes'))


@app.get("/separacoes")
@login_required
@module_required("separacoes")
def listar_separacoes() -> str:
    where, params = separation_visibility_clause()
    separacoes = query_all(
        f"""
        SELECT s.*, st.nome AS store_nome,
               r.nome AS responsavel_nome,
               c.nome AS conferente_nome
        FROM separations s
        JOIN stores st ON st.id = s.store_id
        LEFT JOIN users r ON r.id = s.responsavel_id
        LEFT JOIN users c ON c.id = s.conferente_id
        {where}
        ORDER BY CASE s.status
            WHEN 'ABERTA' THEN 1
            WHEN 'EM_SEPARACAO' THEN 2
            WHEN 'AGUARDANDO_CONFERENCIA' THEN 3
            WHEN 'FINALIZADA' THEN 4
            ELSE 5 END,
            s.id DESC
        """,
        params,
    )
    lotes = lotes_visiveis_para_usuario(g.user)
    return render_template("separacoes.html", title="Separações", separacoes=separacoes, lotes=lotes, usar_conferente=get_setting("usar_conferente", "1") == "1")


def can_access_separation(separation: sqlite3.Row) -> bool:
    if user_is_admin(g.user):
        return True
    return g.user["id"] in {separation["responsavel_id"], separation["conferente_id"]}


def load_separation(separation_id: int) -> sqlite3.Row | None:
    return query_one(
        """
        SELECT s.*, st.nome AS store_nome,
               r.nome AS responsavel_nome,
               c.nome AS conferente_nome,
               creator.nome AS criado_por_nome
        FROM separations s
        JOIN stores st ON st.id = s.store_id
        LEFT JOIN users r ON r.id = s.responsavel_id
        LEFT JOIN users c ON c.id = s.conferente_id
        LEFT JOIN users creator ON creator.id = s.criado_por
        WHERE s.id = ?
        """,
        (separation_id,),
    )




def separation_summary(separation_id: int) -> dict[str, float]:
    row = query_one(
        "SELECT COALESCE(SUM(quantidade_pedida),0) AS qtd_pedida, COALESCE(SUM(quantidade_separada),0) AS qtd_separada FROM separation_items WHERE separation_id = ?",
        (separation_id,),
    )
    return {"qtd_pedida": row["qtd_pedida"], "qtd_separada": row["qtd_separada"]}


@app.get("/separacoes/<int:separation_id>")
@login_required
@module_required("separacoes")
def detalhe_separacao(separation_id: int) -> str | Response:
    separation = load_separation(separation_id)
    if separation is None or not can_access_separation(separation):
        flash("Separação não encontrada ou sem permissão.", "error")
        return redirect(url_for("listar_separacoes"))

    items = query_all(
        """
        SELECT si.*, COALESCE(stk.quantidade_atual, 0) AS estoque_atual
        FROM separation_items si
        LEFT JOIN stock_items stk ON stk.codigo = si.codigo
        WHERE si.separation_id = ?
        ORDER BY si.descricao COLLATE NOCASE ASC, si.id ASC
        """,
        (separation_id,),
    )
    usar_conferente = get_setting("usar_conferente", "1") == "1"
    pode_editar_itens = user_is_admin(g.user) and separation["status"] != "FINALIZADA"
    pode_separar = (user_is_admin(g.user) or (normalize_role(g.user["role"]) == "separador" and g.user["id"] in {separation["responsavel_id"], separation["criado_por"]})) and separation["status"] in {"ABERTA", "EM_SEPARACAO"}
    pode_enviar_conferencia = usar_conferente and (user_is_admin(g.user) or (normalize_role(g.user["role"]) == "separador" and g.user["id"] in {separation["responsavel_id"], separation["criado_por"]})) and separation["status"] in {"ABERTA", "EM_SEPARACAO"}
    pode_finalizar = False
    if usar_conferente:
        pode_finalizar = (user_is_admin(g.user) or normalize_role(g.user["role"]) == "conferente") and (user_is_admin(g.user) or g.user["id"] == separation["conferente_id"]) and separation["status"] == "AGUARDANDO_CONFERENCIA"
        texto_fluxo = "O admin pode lançar itens. O separador marca a quantidade separada. O conferente finaliza."
        texto_botao_finalizar = "Finalizar separação"
    else:
        pode_finalizar = (user_is_admin(g.user) or normalize_role(g.user["role"]) == "separador") and (user_is_admin(g.user) or g.user["id"] == separation["responsavel_id"]) and separation["status"] in {"ABERTA", "EM_SEPARACAO"}
        texto_fluxo = "O admin pode lançar itens. O separador marca a quantidade separada. Como o conferente está desativado, o responsável ou admin finalizam direto."
        texto_botao_finalizar = "Finalizar direto"
    return render_template(
        "detalhe_separacao.html",
        title="Detalhe da separação",
        separation=separation,
        items=items,
        resumo=separation_summary(separation_id),
        pode_editar_itens=pode_editar_itens,
        pode_separar=pode_separar,
        pode_enviar_conferencia=pode_enviar_conferencia,
        pode_finalizar=pode_finalizar,
        texto_fluxo=texto_fluxo,
        texto_botao_finalizar=texto_botao_finalizar,
    )


@app.post("/separacoes/<int:separation_id>/itens")
@login_required
@module_required("separacoes")
@roles_required("admin")
def adicionar_item_separacao(separation_id: int) -> Response:
    separation = load_separation(separation_id)
    if separation is None:
        flash("Separação não encontrada.", "error")
        return redirect(url_for("listar_separacoes"))
    if separation["status"] == "FINALIZADA":
        flash("Não é possível alterar itens de uma separação finalizada.", "error")
        return redirect(url_for("detalhe_separacao", separation_id=separation_id))

    codigo = request.form.get("codigo", "").strip()
    descricao = request.form.get("descricao", "").strip()
    if not codigo or not descricao:
        flash("Informe código e descrição do item.", "error")
        return redirect(url_for("detalhe_separacao", separation_id=separation_id))
    try:
        fator_embalagem = parse_fator_embalagem(request.form.get("fator_embalagem", "1"))
        quantidade_base = parse_float(request.form.get("quantidade_pedida", ""), "Quantidade pedida")
        if quantidade_base <= 0:
            raise ValueError("Quantidade pedida deve ser maior que zero.")
        quantidade_pedida = quantidade_base * fator_embalagem
    except ValueError as exc:
        flash(str(exc), "error")
        return redirect(url_for("detalhe_separacao", separation_id=separation_id))

    stock = query_one("SELECT custo_unitario FROM stock_items WHERE codigo = ?", (codigo,))
    custo_ref = stock["custo_unitario"] if stock else 0
    with closing(get_conn()) as conn:
        conn.execute(
            """
            INSERT INTO separation_items (separation_id, codigo, descricao, fator_embalagem, quantidade_pedida, quantidade_separada, status, custo_unitario_ref, criado_em, atualizado_em)
            VALUES (?, ?, ?, ?, ?, 0, 'PENDENTE', ?, ?, ?)
            """,
            (separation_id, codigo, descricao, fator_embalagem, quantidade_pedida, custo_ref, agora_iso(), agora_iso()),
        )
        conn.commit()
    flash("Item do pedido adicionado.", "success")
    return redirect(url_for("detalhe_separacao", separation_id=separation_id))


@app.post("/separacoes/item/<int:item_id>/atualizar")
@login_required
@module_required("separacoes")
def atualizar_item_separacao(item_id: int) -> Response:
    separation_id = request.form.get("separation_id", "").strip()
    if not separation_id.isdigit():
        flash("Separação inválida.", "error")
        return redirect(url_for("listar_separacoes"))
    separation = load_separation(int(separation_id))
    if separation is None or not can_access_separation(separation):
        flash("Sem acesso a essa separação.", "error")
        return redirect(url_for("listar_separacoes"))
    if separation["status"] not in {"ABERTA", "EM_SEPARACAO"}:
        flash("Essa separação não aceita mais alterações de quantidade.", "error")
        return redirect(url_for("detalhe_separacao", separation_id=separation["id"]))
    if (not user_is_admin(g.user) and normalize_role(g.user["role"]) != "separador") or (not user_is_admin(g.user) and normalize_role(g.user["role"]) == "separador" and g.user["id"] != separation["responsavel_id"]):
        flash("Somente o responsável ou admin podem informar a quantidade separada.", "error")
        return redirect(url_for("detalhe_separacao", separation_id=separation["id"]))

    try:
        qtd_sep = parse_float(request.form.get("quantidade_separada", ""), "Quantidade separada")
    except ValueError as exc:
        flash(str(exc), "error")
        return redirect(url_for("detalhe_separacao", separation_id=separation["id"]))

    with closing(get_conn()) as conn:
        item = conn.execute("SELECT quantidade_pedida FROM separation_items WHERE id = ? AND separation_id = ?", (item_id, separation["id"])).fetchone()
        if item is None:
            flash("Item não encontrado.", "error")
            return redirect(url_for("detalhe_separacao", separation_id=separation["id"]))
        status = "PENDENTE"
        if qtd_sep <= 0:
            status = "PENDENTE"
        elif qtd_sep < float(item["quantidade_pedida"]):
            status = "PARCIAL"
        else:
            status = "SEPARADO"
        conn.execute(
            "UPDATE separation_items SET quantidade_separada = ?, status = ?, atualizado_em = ? WHERE id = ?",
            (qtd_sep, status, agora_iso(), item_id),
        )
        conn.execute("UPDATE separations SET status = 'EM_SEPARACAO' WHERE id = ? AND status = 'ABERTA'", (separation["id"],))
        conn.commit()
    flash("Quantidade separada atualizada.", "success")
    return redirect(url_for("detalhe_separacao", separation_id=separation["id"]))


@app.post("/separacoes/item/<int:item_id>/excluir")
@login_required
@module_required("separacoes")
@roles_required("admin")
def excluir_item_separacao(item_id: int) -> Response:
    separation_id = request.form.get("separation_id", "").strip()
    if not separation_id.isdigit():
        flash("Separação inválida.", "error")
        return redirect(url_for("listar_separacoes"))
    with closing(get_conn()) as conn:
        item = conn.execute("SELECT carryover_source_item_id FROM separation_items WHERE id = ? AND separation_id = ?", (item_id, int(separation_id))).fetchone()
        if item and item["carryover_source_item_id"]:
            conn.execute("UPDATE separation_items SET carryover_copied = 0, atualizado_em = ? WHERE id = ?", (agora_iso(), item["carryover_source_item_id"]))
        conn.execute("DELETE FROM separation_items WHERE id = ? AND separation_id = ?", (item_id, int(separation_id)))
        conn.commit()
    flash("Item removido da separação.", "success")
    return redirect(url_for("detalhe_separacao", separation_id=int(separation_id)))


@app.post("/separacoes/<int:separation_id>/excluir")
@login_required
@module_required("separacoes")
@roles_required("admin")
def excluir_separacao(separation_id: int) -> Response:
    separation = load_separation(separation_id)
    if separation is None:
        flash("Loja da separação não encontrada.", "error")
        return redirect(url_for("listar_separacoes"))
    if separation["status"] == "FINALIZADA":
        flash("Não é possível remover uma loja já finalizada. Use o histórico para manter os dados.", "error")
        return redirect(url_for("listar_separacoes"))

    operacao_chave = lote_operacao_chave_row(separation)
    with closing(get_conn()) as conn:
        desfazer_pendencias_transferidas(conn, separation_id)
        conn.execute("DELETE FROM separations WHERE id = ?", (separation_id,))
        conn.commit()

    restantes = carregar_lote(operacao_chave)
    if restantes:
        flash(f"Loja {separation['store_nome']} removida da separação.", "success")
        return redirect(url_for("grade_lote", lote_codigo=operacao_chave))

    flash("Loja removida. Como era a última do lote, o lote também saiu da lista.", "success")
    return redirect(url_for("listar_separacoes"))


@app.post("/lotes/<lote_codigo>/excluir")
@login_required
@module_required("lotes")
@roles_required("admin")
def excluir_lote(lote_codigo: str) -> Response:
    separacoes = carregar_lote(lote_codigo)
    if not separacoes:
        flash("Lote não encontrado.", "error")
        return redirect(url_for("listar_lotes"))

    finalizadas = [sep["store_nome"] for sep in separacoes if sep["status"] == "FINALIZADA"]
    if finalizadas:
        flash(
            "Não dá para excluir o lote inteiro porque existe loja finalizada nele: " + ", ".join(finalizadas) + ". Remova apenas as lojas abertas.",
            "error",
        )
        return redirect(url_for("grade_lote", lote_codigo=lote_codigo))

    with closing(get_conn()) as conn:
        for sep in separacoes:
            desfazer_pendencias_transferidas(conn, sep["id"])
        conn.execute(
            f"DELETE FROM separations WHERE id IN ({','.join('?' for _ in separacoes)})",
            tuple(sep["id"] for sep in separacoes),
        )
        conn.commit()

    flash(f"Lote removido com {len(separacoes)} loja(s).", "success")
    return redirect(url_for("listar_lotes"))


@app.post("/separacoes/<int:separation_id>/enviar-conferencia")
@login_required
@module_required("separacoes")
def enviar_conferencia(separation_id: int) -> Response:
    separation = load_separation(separation_id)
    if separation is None or not can_access_separation(separation):
        flash("Sem acesso a essa separação.", "error")
        return redirect(url_for("listar_separacoes"))
    if (not user_is_admin(g.user) and normalize_role(g.user["role"]) != "separador") or (not user_is_admin(g.user) and normalize_role(g.user["role"]) == "separador" and g.user["id"] != separation["responsavel_id"]):
        flash("Somente o responsável ou admin podem enviar para conferência.", "error")
        return redirect(url_for("detalhe_separacao", separation_id=separation_id))

    if get_setting("usar_conferente", "1") != "1":
        flash("A função do conferente está desativada. Finalize direto pela própria separação.", "error")
        return redirect(url_for("detalhe_separacao", separation_id=separation_id))

    total_itens = query_one("SELECT COUNT(*) AS c FROM separation_items WHERE separation_id = ?", (separation_id,))["c"]
    if total_itens == 0:
        flash("Adicione itens antes de enviar para conferência.", "error")
        return redirect(url_for("detalhe_separacao", separation_id=separation_id))

    with closing(get_conn()) as conn:
        conn.execute(
            "UPDATE separations SET status = 'AGUARDANDO_CONFERENCIA', enviado_conferencia_em = ? WHERE id = ?",
            (agora_iso(), separation_id),
        )
        conn.commit()
    flash("Separação enviada para conferência.", "success")
    return redirect(url_for("detalhe_separacao", separation_id=separation_id))


def validar_estoque_para_finalizacao(conn: sqlite3.Connection, separation_id: int) -> list[str]:
    problemas: list[str] = []
    itens = conn.execute(
        "SELECT codigo, descricao, quantidade_separada FROM separation_items WHERE separation_id = ?",
        (separation_id,),
    ).fetchall()
    for item in itens:
        stock = conn.execute("SELECT quantidade_atual FROM stock_items WHERE codigo = ?", (item["codigo"],)).fetchone()
        saldo = float(stock["quantidade_atual"]) if stock else 0.0
        if saldo < float(item["quantidade_separada"]):
            problemas.append(f"{item['descricao']} (saldo {fmt_num(saldo)} / separado {fmt_num(item['quantidade_separada'])})")
    return problemas


@app.post("/separacoes/<int:separation_id>/finalizar")
@login_required
@module_required("separacoes")
def finalizar_separacao(separation_id: int) -> Response:
    separation = load_separation(separation_id)
    if separation is None or not can_access_separation(separation):
        flash("Sem acesso a essa separação.", "error")
        return redirect(url_for("listar_separacoes"))

    usar_conferente = get_setting("usar_conferente", "1") == "1"
    if usar_conferente:
        if separation["status"] != "AGUARDANDO_CONFERENCIA":
            flash("A separação precisa estar aguardando conferência.", "error")
            return redirect(url_for("detalhe_separacao", separation_id=separation_id))
        if (not user_is_admin(g.user) and normalize_role(g.user["role"]) != "conferente") or (not user_is_admin(g.user) and normalize_role(g.user["role"]) == "conferente" and g.user["id"] != separation["conferente_id"]):
            flash("Somente o conferente designado ou admin podem finalizar.", "error")
            return redirect(url_for("detalhe_separacao", separation_id=separation_id))
    else:
        if separation["status"] not in {"ABERTA", "EM_SEPARACAO"}:
            flash("Com o conferente desligado, só é possível finalizar separações ainda em andamento.", "error")
            return redirect(url_for("detalhe_separacao", separation_id=separation_id))
        if (not user_is_admin(g.user) and normalize_role(g.user["role"]) != "separador") or (not user_is_admin(g.user) and normalize_role(g.user["role"]) == "separador" and g.user["id"] != separation["responsavel_id"]):
            flash("Somente o responsável ou admin podem finalizar quando o conferente estiver desligado.", "error")
            return redirect(url_for("detalhe_separacao", separation_id=separation_id))

    with closing(get_conn()) as conn:
        itens = conn.execute("SELECT * FROM separation_items WHERE separation_id = ?", (separation_id,)).fetchall()
        if not itens:
            flash("Essa separação não possui itens.", "error")
            return redirect(url_for("detalhe_separacao", separation_id=separation_id))

        usar_controle_global = get_setting("vincular_estoque", "1") == "1"
        precisa_abater = usar_controle_global and bool(separation["usar_estoque"])

        if precisa_abater:
            problemas = validar_estoque_para_finalizacao(conn, separation_id)
            if problemas:
                flash("Saldo insuficiente para finalizar com controle de estoque: " + "; ".join(problemas), "error")
                return redirect(url_for("detalhe_separacao", separation_id=separation_id))

            for item in itens:
                stock = conn.execute("SELECT * FROM stock_items WHERE codigo = ?", (item["codigo"],)).fetchone()
                if stock is None:
                    conn.execute(
                        "INSERT INTO stock_items (codigo, descricao, quantidade_atual, custo_unitario, atualizado_em) VALUES (?, ?, 0, ?, ?)",
                        (item["codigo"], item["descricao"], item["custo_unitario_ref"], agora_iso()),
                    )
                    stock = conn.execute("SELECT * FROM stock_items WHERE codigo = ?", (item["codigo"],)).fetchone()
                novo_saldo = float(stock["quantidade_atual"]) - float(item["quantidade_separada"])
                conn.execute(
                    "UPDATE stock_items SET quantidade_atual = ?, atualizado_em = ? WHERE id = ?",
                    (novo_saldo, agora_iso(), stock["id"]),
                )
                conn.execute(
                    "INSERT INTO stock_movements (stock_item_id, tipo, quantidade, observacao, referencia_tipo, referencia_id, criado_por, criado_em) VALUES (?, 'SAIDA_SEPARACAO', ?, ?, 'SEPARACAO', ?, ?, ?)",
                    (stock["id"], -float(item["quantidade_separada"]), f"Saída da separação {separation['lote_nome']} - {separation['store_nome']}", separation_id, g.user["id"], agora_iso()),
                )

        pendencias_restantes = sum(
            max(float(item["quantidade_pedida"] or 0) - float(item["quantidade_separada"] or 0), 0)
            for item in itens
        )
        conn.execute(
            "UPDATE separations SET status = 'FINALIZADA', finalizado_em = ? WHERE id = ?",
            (agora_iso(), separation_id),
        )
        conn.commit()

    if pendencias_restantes > 0:
        flash(
            "Separação finalizada parcialmente. O restante pendente poderá ser puxado automaticamente no próximo dia ao criar uma nova separação para essa loja.",
            "success",
        )
    else:
        flash("Separação finalizada e registrada no histórico.", "success")
    return redirect(url_for("detalhe_separacao", separation_id=separation_id))




@app.post("/relatorios/<int:separation_id>/apagar")
@login_required
@module_required("relatorios")
@roles_required("admin")
def apagar_historico_separacao(separation_id: int) -> Response:
    with closing(get_conn()) as conn:
        try:
            separation = conn.execute("SELECT status FROM separations WHERE id = ?", (separation_id,)).fetchone()
            if separation is None:
                raise ValueError("Separação não encontrada.")
            if separation["status"] == "CANCELADA":
                excluir_separacao_cancelada_no_conn(conn, separation_id)
                mensagem = "Separação cancelada excluída em definitivo."
            else:
                apagar_historico_separacao_no_conn(conn, separation_id, g.user["id"])
                mensagem = "Registro removido do histórico. Se usava estoque, o saldo foi estornado."
            conn.commit()
        except ValueError as exc:
            flash(str(exc), "error")
            return redirect(url_for("relatorios"))
    flash(mensagem, "success")
    return redirect(url_for("relatorios"))


@app.post("/relatorios/lotes/<lote_codigo>/apagar")
@login_required
@module_required("relatorios")
@roles_required("admin")
def apagar_historico_lote(lote_codigo: str) -> Response:
    separacoes = [s for s in carregar_lote_completo(lote_codigo) if s["status"] in {"FINALIZADA", "CANCELADA"}]
    if not separacoes:
        flash("Nenhum registro histórico encontrado nesse lote.", "error")
        return redirect(url_for("relatorios"))
    canceladas = 0
    excluidas = 0
    with closing(get_conn()) as conn:
        try:
            for sep in separacoes:
                if sep["status"] == "CANCELADA":
                    excluir_separacao_cancelada_no_conn(conn, sep["id"])
                    excluidas += 1
                else:
                    apagar_historico_separacao_no_conn(conn, sep["id"], g.user["id"])
                    canceladas += 1
            conn.commit()
        except ValueError as exc:
            flash(str(exc), "error")
            return redirect(url_for("relatorios"))
    partes = []
    if canceladas:
        partes.append(f"{canceladas} loja(s) cancelada(s) com estorno")
    if excluidas:
        partes.append(f"{excluidas} loja(s) cancelada(s) excluída(s) em definitivo")
    flash("Lote processado: " + "; ".join(partes) + ".", "success")
    return redirect(url_for("relatorios"))


@app.get("/relatorios/lotes/<lote_codigo>")
@login_required
def detalhe_historico_lote(lote_codigo: str) -> str | Response:
    if not (user_has_access(g.user, "relatorios") and (user_is_admin(g.user) or normalize_role(g.user["role"]) == "balanco")):
        return forbidden_redirect("Sem permissão para acessar o histórico.")
    historico = [s for s in carregar_lote_completo(lote_codigo) if s["status"] in {"FINALIZADA", "CANCELADA"}]
    if not historico:
        flash("Lote histórico não encontrado.", "error")
        return redirect(url_for("relatorios"))
    itens_por_separacao: dict[int, list[sqlite3.Row]] = {}
    for sep in historico:
        itens_por_separacao[sep["id"]] = query_all(
            "SELECT codigo, descricao, fator_embalagem, quantidade_pedida, quantidade_separada, custo_unitario_ref FROM separation_items WHERE separation_id = ? ORDER BY descricao COLLATE NOCASE ASC",
            (sep["id"],),
        )
    return render_template(
        "historico_lote.html",
        title="Histórico do lote",
        historico=historico,
        itens_por_separacao=itens_por_separacao,
    )


@app.get("/relatorios")
@login_required
@module_required("relatorios")
def relatorios() -> str:
    if not (user_has_access(g.user, "relatorios") and (user_is_admin(g.user) or normalize_role(g.user["role"]) == "balanco")):
        return forbidden_redirect("Somente admin ou balanço podem acessar os relatórios.")
    historico_lotes = query_all(
        f"""
        SELECT {lote_operacao_chave_expr('s')} AS operacao_chave,
               s.lote_nome,
               s.data_referencia,
               MAX(COALESCE(s.finalizado_em, s.criado_em)) AS finalizado_em,
               COUNT(*) AS total_lojas,
               GROUP_CONCAT(st.nome, ' • ') AS lojas,
               COALESCE(SUM(si.quantidade_separada), 0) AS qtd_total,
               COALESCE(SUM(si.quantidade_separada * si.custo_unitario_ref), 0) AS custo_total,
               SUM(CASE WHEN s.status = 'FINALIZADA' THEN 1 ELSE 0 END) AS lojas_finalizadas,
               SUM(CASE WHEN s.status = 'CANCELADA' THEN 1 ELSE 0 END) AS lojas_canceladas,
               CASE
                   WHEN SUM(CASE WHEN s.status = 'FINALIZADA' THEN 1 ELSE 0 END) > 0 AND SUM(CASE WHEN s.status = 'CANCELADA' THEN 1 ELSE 0 END) > 0 THEN 'MISTO'
                   WHEN SUM(CASE WHEN s.status = 'CANCELADA' THEN 1 ELSE 0 END) > 0 THEN 'CANCELADO'
                   ELSE 'FINALIZADO'
               END AS status_lote
        FROM separations s
        JOIN stores st ON st.id = s.store_id
        LEFT JOIN separation_items si ON si.separation_id = s.id
        WHERE s.status IN ('FINALIZADA', 'CANCELADA')
        GROUP BY operacao_chave, s.lote_nome, s.data_referencia
        ORDER BY MAX(COALESCE(s.finalizado_em, s.criado_em)) DESC, MAX(s.id) DESC
        LIMIT 100
        """
    )
    resumo = query_one(
        """
        SELECT COUNT(DISTINCT s.id) AS finalizadas,
               COALESCE(SUM(si.quantidade_separada), 0) AS itens,
               COALESCE(SUM(si.quantidade_separada * si.custo_unitario_ref), 0) AS custo
        FROM separations s
        LEFT JOIN separation_items si ON si.separation_id = s.id
        WHERE s.status = 'FINALIZADA'
        """
    )
    return render_template("relatorios.html", title="Relatórios", historico_lotes=historico_lotes, resumo=resumo)


@app.get("/estoque/historico/exportar.xlsx")
@login_required
@module_required("estoque")
def exportar_historico_estoque_excel() -> Response:
    filters = sanitize_stock_history_filters(request.args)
    movimentos = fetch_stock_movements(filters, limit=None)

    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font
    except ImportError:
        flash("Para exportar em Excel, instale a dependência openpyxl.", "error")
        return redirect(url_for("estoque", **request.args.to_dict(flat=True)))

    wb = Workbook()
    ws = wb.active
    ws.title = "Histórico estoque"

    filtros_aplicados = stock_history_filter_labels(filters)
    ws.append(["Histórico de movimentações do estoque"])
    ws.append(["Gerado em", agora_br()])
    ws.append(["Filtros", " | ".join(filtros_aplicados) if filtros_aplicados else "Sem filtros específicos"])
    ws.append([])

    headers = [
        "Data/Hora",
        "Tipo",
        "Código",
        "Código de barras",
        "Descrição",
        "Quantidade",
        "Usuário",
        "Observação",
        "Referência",
    ]
    ws.append(headers)
    for cell in ws[5]:
        cell.font = Font(bold=True)

    for mov in movimentos:
        usuario = mov["usuario_nome"] or mov["usuario_login"] or "Sistema"
        referencia = f"{mov['referencia_tipo'] or '-'} {mov['referencia_id'] or ''}".strip()
        ws.append([
            mov["criado_em"],
            stock_movement_label(mov["tipo"]),
            mov["codigo"],
            mov["codigo_barras"] or "",
            mov["descricao"],
            float(mov["quantidade"] or 0),
            usuario,
            mov["observacao"] or "",
            referencia,
        ])

    column_widths = {
        "A": 19,
        "B": 22,
        "C": 14,
        "D": 18,
        "E": 42,
        "F": 14,
        "G": 24,
        "H": 36,
        "I": 18,
    }
    for col, width in column_widths.items():
        ws.column_dimensions[col].width = width

    buffer = io.BytesIO()
    wb.save(buffer)
    buffer.seek(0)
    return send_file(
        buffer,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=stock_history_export_filename("xlsx"),
    )


@app.get("/estoque/historico/exportar.pdf")
@login_required
@module_required("estoque")
def exportar_historico_estoque_pdf() -> Response:
    filters = sanitize_stock_history_filters(request.args)
    movimentos = fetch_stock_movements(filters, limit=None)

    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.platypus import LongTable, Paragraph, SimpleDocTemplate, Spacer, TableStyle
    except ImportError:
        flash("Para exportar em PDF, instale a dependência reportlab.", "error")
        return redirect(url_for("estoque", **request.args.to_dict(flat=True)))

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=landscape(A4),
        leftMargin=18,
        rightMargin=18,
        topMargin=18,
        bottomMargin=18,
    )
    styles = getSampleStyleSheet()
    story = [
        Paragraph("Histórico de movimentações do estoque", styles["Title"]),
        Paragraph(f"Gerado em: {agora_br()}", styles["Normal"]),
    ]

    filtros_aplicados = stock_history_filter_labels(filters)
    story.append(Paragraph("Filtros: " + (" | ".join(filtros_aplicados) if filtros_aplicados else "Sem filtros específicos"), styles["Normal"]))
    story.append(Spacer(1, 10))

    data = [["Data/Hora", "Tipo", "Código", "Descrição", "Qtd.", "Usuário", "Observação"]]
    for mov in movimentos:
        usuario = mov["usuario_nome"] or mov["usuario_login"] or "Sistema"
        descricao = f"{mov['descricao']} ({mov['codigo']})"
        data.append([
            mov["criado_em"],
            stock_movement_label(mov["tipo"]),
            mov["codigo"],
            descricao,
            fmt_num(mov["quantidade"]),
            usuario,
            mov["observacao"] or "-",
        ])

    table = LongTable(data, repeatRows=1, colWidths=[80, 95, 55, 240, 50, 110, 130])
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f2937")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#cbd5e1")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.HexColor("#eef2f7")]),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(table)

    doc.build(story)
    buffer.seek(0)
    return send_file(
        buffer,
        mimetype="application/pdf",
        as_attachment=True,
        download_name=stock_history_export_filename("pdf"),
    )


@app.get("/api/produto")
@login_required
@module_required("estoque")
def api_produto() -> Response:
    termo = request.args.get("codigo", "").strip()
    if not termo:
        return jsonify({"ok": False, "descricao": "", "quantidade_atual": 0})

    item = query_one(
        """
        SELECT *
        FROM stock_items
        WHERE ativo = 1
          AND (codigo = ? OR codigo_barras = ?)
        LIMIT 1
        """,
        (termo, termo),
    )

    if item is None:
        return jsonify({"ok": False, "descricao": "", "quantidade_atual": 0})

    return jsonify(
        {
            "ok": True,
            "codigo": item["codigo"],
            "codigo_barras": item["codigo_barras"],
            "descricao": item["descricao"],
            "fator_embalagem": item["fator_embalagem"],
            "quantidade_atual": item["quantidade_atual"],
            "custo_unitario": item["custo_unitario"],
        }
    )


@app.get("/health")
def health() -> Response:
    return jsonify({"status": "ok", "db": DB_PATH})


if __name__ == "__main__":
    ensure_default_data()
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)
