from __future__ import annotations

import os
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
    conn.execute("CREATE INDEX IF NOT EXISTS idx_separations_lote_codigo ON separations(lote_codigo)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stock_items_codigo_barras ON stock_items(codigo_barras)")
    conn.execute("UPDATE separations SET lote_codigo = 'SEP-' || id WHERE lote_codigo IS NULL OR TRIM(lote_codigo) = ''")
    conn.execute("UPDATE stock_items SET fator_embalagem = 1 WHERE fator_embalagem IS NULL OR fator_embalagem <= 0")
    conn.execute("UPDATE separation_items SET fator_embalagem = 1 WHERE fator_embalagem IS NULL OR fator_embalagem <= 0")
    conn.execute("UPDATE separation_items SET carryover_copied = 0 WHERE carryover_copied IS NULL")


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
    def decorator(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            if g.user is None:
                return redirect(url_for("login"))
            if g.user["role"] not in roles:
                flash("Você não tem permissão para acessar essa área.", "error")
                return redirect(url_for("dashboard"))
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


app.jinja_env.globals.update(fmt_num=fmt_num, fmt_money=fmt_money, fmt_fator_embalagem=fmt_fator_embalagem, quantidade_em_embalagens=quantidade_em_embalagens, role_badge=role_badge, status_class=status_class, lote_operacao_chave_row=lote_operacao_chave_row)




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
        return redirect(url_for("dashboard"))

    return render_template("login.html", title="Login")


@app.get("/logout")
def logout() -> Response:
    session.clear()
    flash("Sessão encerrada.", "success")
    return redirect(url_for("login"))


def ultimos_lotes_resumo(limit: int = 8, include_canceladas: bool = True) -> list[sqlite3.Row]:
    chave_expr = lote_operacao_chave_expr("s")
    where_parts = ["1=1"]
    if not include_canceladas:
        where_parts.append("s.status <> 'CANCELADA'")
    if g.user["role"] == "separador":
        where_parts.append("s.responsavel_id = ?")
        params: list[Any] = [g.user["id"], limit]
    elif g.user["role"] == "conferente":
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
    if user and user["role"] == "separador":
        where_clauses.append("s.responsavel_id = ?")
        params.append(user["id"])
    elif user and user["role"] == "conferente":
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
@roles_required("admin")
def usuarios() -> str | Response:
    if request.method == "POST":
        nome = request.form.get("nome", "").strip()
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        role = request.form.get("role", "separador").strip()
        if not nome or not username or not password or role not in {"admin", "separador", "conferente", "balanco"}:
            flash("Preencha os dados do usuário corretamente.", "error")
            return redirect(url_for("usuarios"))
        try:
            with closing(get_conn()) as conn:
                conn.execute(
                    "INSERT INTO users (nome, username, password_hash, role, ativo, criado_em) VALUES (?, ?, ?, ?, 1, ?)",
                    (nome, username, generate_password_hash(password), role, agora_iso()),
                )
                conn.commit()
            flash("Usuário criado com sucesso.", "success")
        except sqlite3.IntegrityError:
            flash("Esse login já existe.", "error")
        return redirect(url_for("usuarios"))

    users = query_all("SELECT * FROM users ORDER BY ativo DESC, role, nome")
    return render_template("usuarios.html", title="Usuários", users=users)


@app.post("/usuarios/<int:user_id>/alternar")
@login_required
@roles_required("admin")
def alternar_usuario(user_id: int) -> Response:
    if user_id == g.user["id"]:
        flash("Você não pode desativar seu próprio usuário por aqui.", "error")
        return redirect(url_for("usuarios"))
    with closing(get_conn()) as conn:
        row = conn.execute("SELECT ativo FROM users WHERE id = ?", (user_id,)).fetchone()
        if row is None:
            flash("Usuário não encontrado.", "error")
            return redirect(url_for("usuarios"))
        novo = 0 if row["ativo"] else 1
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

        outros_admins = conn.execute("SELECT COUNT(*) AS c FROM users WHERE role = 'admin' AND id <> ?", (user_id,)).fetchone()["c"]
        if user["role"] == "admin" and int(outros_admins or 0) == 0:
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
def estoque() -> str | Response:
    if g.user["role"] not in {"admin", "balanco"}:
        flash("Somente admin ou balanço podem alterar o estoque.", "error")
        return redirect(url_for("dashboard"))

    if request.method == "POST":
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

    termo = request.args.get("q", "").strip()
    somente_com_saldo = request.args.get("somente_com_saldo", "0") == "1"
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

    return render_template(
        "estoque.html",
        title="Estoque",
        stock_items=stock_items,
        termo_busca=termo,
        somente_com_saldo=somente_com_saldo,
        busca_realizada=busca_realizada,
    )


@app.post("/estoque/<int:stock_item_id>/editar")
@login_required
def editar_item_estoque(stock_item_id: int) -> Response:
    if g.user["role"] not in {"admin", "balanco"}:
        flash("Sem permissão para editar item do estoque.", "error")
        return redirect(url_for("dashboard"))

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
def ajustar_estoque(stock_item_id: int) -> Response:
    if g.user["role"] not in {"admin", "balanco"}:
        flash("Sem permissão para ajustar estoque.", "error")
        return redirect(url_for("dashboard"))

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
def remover_item_estoque(stock_item_id: int) -> Response:
    if g.user["role"] not in {"admin", "balanco"}:
        flash("Sem permissão para remover item do estoque.", "error")
        return redirect(url_for("dashboard"))

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
    if user is None or user["role"] == "admin":
        return todos
    resultado: list[sqlite3.Row] = []
    for lote in todos:
        separacoes = carregar_lote(lote["operacao_chave"])
        if user["role"] == "separador" and any(sep["responsavel_id"] == user["id"] for sep in separacoes):
            resultado.append(lote)
        elif user["role"] == "conferente" and any(sep["conferente_id"] == user["id"] for sep in separacoes):
            resultado.append(lote)
    return resultado


def pode_acessar_lote_operacao(separacoes: list[sqlite3.Row], modo: str) -> bool:
    if g.user is None:
        return False
    if g.user["role"] == "admin":
        return True
    if modo == "separacao":
        return g.user["role"] == "separador" and any(sep["responsavel_id"] == g.user["id"] for sep in separacoes)
    if modo == "conferencia":
        return g.user["role"] == "conferente" and any(sep["conferente_id"] == g.user["id"] for sep in separacoes)
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
    if user["role"] == "admin":
        return "WHERE s.status <> 'CANCELADA'", []
    if user["role"] == "separador":
        return "WHERE s.status <> 'CANCELADA' AND s.responsavel_id = ?", [user["id"]]
    return "WHERE s.status <> 'CANCELADA' AND (s.conferente_id = ? OR s.responsavel_id = ?)", [user["id"], user["id"]]




@app.get("/lotes")
@login_required
@roles_required("admin")
def listar_lotes() -> str:
    return render_template("lotes.html", title="Lotes", lotes=listar_lotes_em_aberto())




@app.route("/lotes/<lote_codigo>/grade", methods=["GET", "POST"])
@login_required
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
def separar_itens_lote(lote_codigo: str) -> str | Response:
    separacoes = carregar_lote(lote_codigo)
    if not separacoes or not pode_acessar_lote_operacao(separacoes, "separacao"):
        flash("Lote não encontrado ou sem permissão para separar.", "error")
        return redirect(url_for("listar_separacoes"))

    produtos = itens_do_lote_para_fluxo(lote_codigo, separacoes)
    if not produtos:
        flash("Esse lote ainda não possui itens para separar.", "error")
        return redirect(url_for("grade_lote", lote_codigo=lote_codigo) if g.user["role"] == "admin" else url_for("listar_separacoes"))

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
    if g.user["role"] == "admin":
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
    pode_editar_itens = g.user["role"] == "admin" and separation["status"] != "FINALIZADA"
    pode_separar = g.user["role"] in {"admin", "separador"} and g.user["id"] in {separation["responsavel_id"], separation["criado_por"]} and separation["status"] in {"ABERTA", "EM_SEPARACAO"}
    pode_enviar_conferencia = usar_conferente and g.user["role"] in {"admin", "separador"} and g.user["id"] in {separation["responsavel_id"], separation["criado_por"]} and separation["status"] in {"ABERTA", "EM_SEPARACAO"}
    pode_finalizar = False
    if usar_conferente:
        pode_finalizar = g.user["role"] in {"admin", "conferente"} and (g.user["role"] == "admin" or g.user["id"] == separation["conferente_id"]) and separation["status"] == "AGUARDANDO_CONFERENCIA"
        texto_fluxo = "O admin pode lançar itens. O separador marca a quantidade separada. O conferente finaliza."
        texto_botao_finalizar = "Finalizar separação"
    else:
        pode_finalizar = g.user["role"] in {"admin", "separador"} and (g.user["role"] == "admin" or g.user["id"] == separation["responsavel_id"]) and separation["status"] in {"ABERTA", "EM_SEPARACAO"}
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
    if g.user["role"] not in {"admin", "separador"} or (g.user["role"] == "separador" and g.user["id"] != separation["responsavel_id"]):
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
def enviar_conferencia(separation_id: int) -> Response:
    separation = load_separation(separation_id)
    if separation is None or not can_access_separation(separation):
        flash("Sem acesso a essa separação.", "error")
        return redirect(url_for("listar_separacoes"))
    if g.user["role"] not in {"admin", "separador"} or (g.user["role"] == "separador" and g.user["id"] != separation["responsavel_id"]):
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
        if g.user["role"] not in {"admin", "conferente"} or (g.user["role"] == "conferente" and g.user["id"] != separation["conferente_id"]):
            flash("Somente o conferente designado ou admin podem finalizar.", "error")
            return redirect(url_for("detalhe_separacao", separation_id=separation_id))
    else:
        if separation["status"] not in {"ABERTA", "EM_SEPARACAO"}:
            flash("Com o conferente desligado, só é possível finalizar separações ainda em andamento.", "error")
            return redirect(url_for("detalhe_separacao", separation_id=separation_id))
        if g.user["role"] not in {"admin", "separador"} or (g.user["role"] == "separador" and g.user["id"] != separation["responsavel_id"]):
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
    if g.user["role"] not in {"admin", "balanco"}:
        flash("Sem permissão para acessar o histórico.", "error")
        return redirect(url_for("dashboard"))
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
def relatorios() -> str:
    if g.user["role"] not in {"admin", "balanco"}:
        flash("Somente admin ou balanço podem acessar os relatórios.", "error")
        return redirect(url_for("dashboard"))
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


@app.get("/api/produto")
@login_required
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
