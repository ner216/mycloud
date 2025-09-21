import sqlite3
from contextlib import contextmanager


class database(object):
    def __init__(self, db_path: str):
        self.db_path = db_path # Path to SQLite db file
        self.create_tables()

    # Create context manager method to create and return connections/cursors
    @contextmanager
    def  _get_cursur(self, commit: bool = False):
        conn = None
        cursor = None

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            yield cursor
            if commit:
                conn.commit()
        except sqlite3.Error as e:
            print(f"[ERR] DB error occured:\n{e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    # Create the files table
    def create_tables(self):
        with self._get_cursor(commit=True) as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS files (
                drive_file_id TEXT PRIMARY KEY,
                path TEXT,
                title TEXT,
                last_modified REAL,
                size INTEGER,
                sync_status TEXT,
                file_hash TEXT,
                parent_id TEXT
                )

                CREATE TABLE IF NOT EXISTS folders (
                drive_file_id TEXT PRIMARY KEY,
                path TEXT,
                title TEXT,
                parent_id TEXT
                )
            """)

    # Create or update an existing file entry
    def add_or_update_file(
        self, 
        path: str, 
        last_modified: float, 
        size: int, 
        sync_stat: str, 
        drive_file_id: str, 
        file_hash: str, 
        parent_id: str
        ):
        query = (
            "INSERT OR REPLACE INTO files"
            "(drive_file_id,"
            "path,"
            "title,"
            "last_modified,"
            "size,"
            "sync_status,"
            "file_hash,"
            "parent_id)"
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )

        with self._get_cursor(commit=True) as cursor:
            cursor.execute(
                query, 
                (
                    drive_file_id, 
                    path, 
                    title, 
                    last_modified, 
                    size, 
                    sync_stat, 
                    drive_file_id, 
                    file_hash, 
                    parent_id
                ))

    def add_or_update_folder(drive_file_id: str, path: str, title: str, parent_id: str):
        with self._get_cursor(commit=True) as cursor:
            cursor.execute(
                "INSERT OR REPLACE INTO folders (drive_file_id, path, title, parent_id) VALUES (?, ?, ?, ?)",
                (drive_file_id, path, title, parent_id)
            )

    # Retrieve file data given its path
    def get_file(self, path: str) -> Dict[str, Any] | None:
        query = (
            "SELECT drive_file_id,"
            "path,"
            "title,"
            "last_modified,"
            "size,"
            "sync_status,"
            "file_hash,"
            "parent_id"
            "FROM files WHERE path = ?"
        )
        with self._get_cursor(commit=False) as cursor:
            cursor.execute(query, (path,))
            row = cursor.fetchone()
            if row:
                return {
                    "drive_file_id": row[0]
                    "path": row[1],
                    "title": row[2]
                    "last_modified", row[3],
                    "size": row[4],
                    "sync_status": row[5],
                    "drive_file_id": row[6],
                    "file_hash": row[7],
                    "parent_id": row[8]
                }
            return None

    def get_folder(self, drive_file_id: str) -> dict:
        query = (
            "SELECT drive_file_id,"
            "path,"
            "title,"
            "parent_id"
            "FROM folders WHERE drive_file_id = ?"
        )
        with self._get_cursor(commit=False) as cursor:
            cursor.execute(query, (drive_file_id,))
            row = cursor.fetchone()
            if row:
                return {
                    "drive_file_id": row[0],
                    "path": row[1],
                    "title": row[2],
                    "parent_id": row[3]
                }
            return None

    # Return a list of all file entried in the files table
    def get_all_files(self) -> list:
        query = (
            "SELECT drive_file_id,"
            "path,"
            "title,"
            "last_modified,"
            "size,"
            "sync_status,"
            "file_hash,"
            "parent_id"
            "FROM files"
        )
        with self._get_cursor(commit=False) as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            return [{
                "drive_file_id": row[0], 
                "path": row[1], 
                "title": row[2], 
                "last_modified": row[3], 
                "size": row[4], 
                "sync_status": row[5], 
                "file_hash": row[7],
                "parent_id": row[8]
                }
                for row in rows
            ]

    def get_all_folders(self) -> list:
        query = (
            "SELECT drive_file_id,"
            "path,"
            "title,"
            "parent_id"
            "FROM folders"
        )
        with self._get_cursor(commit=False) as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            return [{
                "drive_file_id": row[0],
                "path": row[1],
                "title": row[2],
                "parent_id": row[3]
                }
                for row in rows
            ]

    # Delete a single file given its id
    def delete_file(self, drive_file_id: str) -> None:
        with self._get_cursor(commit=True) as cursor:
            cursor.execute(
                "DELETE FROM files WHERE drive_file_id = ?",
                (drive_file_id,)
                )

    # Delete a single folder given its id
    def delete_folder(self, drive_file_id: str) -> None:
        with self._get_cursor(commit=True) as cursor:
            cursor.execute(
                "DELETE FROM folders WHERE drive_file_id = ?",
                (drive_file_id,)
                )