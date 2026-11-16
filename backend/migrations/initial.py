from sqlalchemy import inspect, text
from src.config.database import Base, engine
from src import models
def _ensure_column(table, column, ddl):
    try:
        cols=[c['name'] for c in inspect(engine).get_columns(table)]
    except Exception:
        return
    if column not in cols:
        with engine.begin() as conn:conn.execute(text(f'ALTER TABLE {table} ADD COLUMN {ddl}'))
def migrate():
    Base.metadata.create_all(engine)
    _ensure_column('jobs','epoch','epoch INTEGER NOT NULL DEFAULT 0')
    _ensure_column('reports','epoch','epoch INTEGER NOT NULL DEFAULT 0')
    verb='INSERT IGNORE' if engine.dialect.name=='mysql' else 'INSERT OR IGNORE'
    with engine.begin() as conn:
        conn.execute(text(f"{verb} INTO site_state (name, value) VALUES ('epoch','0'),('restore_lock','1')"))
if __name__=='__main__':migrate()
