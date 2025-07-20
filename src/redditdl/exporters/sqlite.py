"""
SQLite Exporter

SQLite database exporter with automatic schema generation, indexing,
and data analysis optimization for Reddit post metadata.
"""

import sqlite3
import json
import time
import gzip
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from datetime import datetime

from .base import BaseExporter, ExportResult, FormatInfo


class SqliteExporter(BaseExporter):
    """
    SQLite database exporter for Reddit post data.
    
    Features:
    - Automatic schema generation from data structure
    - Optimized indexing for common queries
    - Support for normalized and denormalized schemas
    - Full-text search capabilities
    - Data integrity constraints
    - Incremental updates support
    - Query optimization suggestions
    """
    
    def _create_format_info(self) -> FormatInfo:
        """Create format information for SQLite export."""
        return FormatInfo(
            name="sqlite",
            extension=".db",
            description="SQLite database with automatic schema generation",
            mime_type="application/x-sqlite3",
            supports_compression=False,
            supports_streaming=False,
            supports_incremental=True,
            max_records=None,  # No practical limit
            schema_required=False
        )
    
    def _create_config_schema(self) -> Dict[str, Any]:
        """Create configuration schema for SQLite export."""
        return {
            'schema_mode': {
                'type': 'string',
                'default': 'auto',
                'choices': ['auto', 'normalized', 'denormalized', 'flat'],
                'description': 'Database schema structure mode'
            },
            'table_prefix': {
                'type': 'string',
                'default': 'reddit_',
                'description': 'Prefix for database table names'
            },
            'create_indexes': {
                'type': 'boolean',
                'default': True,
                'description': 'Create performance indexes'
            },
            'enable_fts': {
                'type': 'boolean',
                'default': True,
                'description': 'Enable full-text search on text columns'
            },
            'batch_size': {
                'type': 'integer',
                'default': 1000,
                'minimum': 100,
                'maximum': 10000,
                'description': 'Number of records per batch insert'
            },
            'journal_mode': {
                'type': 'string',
                'default': 'WAL',
                'choices': ['DELETE', 'TRUNCATE', 'PERSIST', 'MEMORY', 'WAL', 'OFF'],
                'description': 'SQLite journal mode'
            },
            'synchronous': {
                'type': 'string',
                'default': 'NORMAL',
                'choices': ['OFF', 'NORMAL', 'FULL', 'EXTRA'],
                'description': 'SQLite synchronous mode'
            },
            'foreign_keys': {
                'type': 'boolean',
                'default': True,
                'description': 'Enable foreign key constraints'
            },
            'compress_text': {
                'type': 'boolean',
                'default': False,
                'description': 'Compress large text fields'
            },
            'date_format': {
                'type': 'string',
                'default': 'iso',
                'choices': ['iso', 'timestamp', 'readable'],
                'description': 'Format for date storage'
            },
            'vacuum': {
                'type': 'boolean',
                'default': True,
                'description': 'Vacuum database after export'
            },
            'analyze': {
                'type': 'boolean',
                'default': True,
                'description': 'Analyze tables for query optimization'
            },
            'backup_existing': {
                'type': 'boolean',
                'default': True,
                'description': 'Backup existing database before overwrite'
            }
        }
    
    def export(self, data: Dict[str, Any], output_path: str, config: Dict[str, Any]) -> ExportResult:
        """Export data to SQLite database."""
        start_time = time.time()
        result = ExportResult(format_name="sqlite")
        
        try:
            # Validate input data
            validation_errors = self.validate_data(data)
            if validation_errors:
                for error in validation_errors:
                    result.add_error(error)
                return result
            
            # Prepare output path
            output_file = self.prepare_output_path(output_path, config)
            
            # Backup existing database if requested
            if output_file.exists() and config.get('backup_existing', True):
                self._backup_database(output_file)
            
            # Extract posts data
            posts = data.get('posts', [])
            if not posts:
                result.add_warning("No posts to export")
                # Create empty database with schema
                self._create_empty_database(output_file, config)
                result.output_path = str(output_file)
                return result
            
            # Create database and schema
            with sqlite3.connect(str(output_file)) as conn:
                self._configure_database(conn, config)
                
                # Determine schema mode and create tables
                schema_mode = config.get('schema_mode', 'auto')
                if schema_mode == 'auto':
                    schema_mode = self._determine_optimal_schema(posts)
                
                if schema_mode == 'normalized':
                    self._create_normalized_schema(conn, posts, config)
                    self._insert_normalized_data(conn, posts, data, config, result)
                elif schema_mode == 'denormalized':
                    self._create_denormalized_schema(conn, posts, config)
                    self._insert_denormalized_data(conn, posts, data, config, result)
                else:  # flat
                    self._create_flat_schema(conn, posts, config)
                    self._insert_flat_data(conn, posts, data, config, result)
                
                # Create indexes
                if config.get('create_indexes', True):
                    self._create_indexes(conn, config)
                
                # Enable full-text search
                if config.get('enable_fts', True):
                    self._create_fts_tables(conn, config)
                
                # Optimize database
                if config.get('analyze', True):
                    self._analyze_database(conn)
                
                if config.get('vacuum', True):
                    self._vacuum_database(conn)
            
            result.output_path = str(output_file)
            result.records_exported = len(posts)
            result.execution_time = time.time() - start_time
            
            # Get file size
            if output_file.exists():
                result.file_size = output_file.stat().st_size
            
            # Add metadata
            result.metadata['schema_mode'] = schema_mode
            result.metadata['database_path'] = str(output_file)
            
            self.logger.info(f"SQLite export completed: {result.records_exported} records to {output_file}")
            
        except Exception as e:
            result.add_error(f"SQLite export failed: {e}")
            self.logger.error(f"SQLite export error: {e}")
        
        return result
    
    def _configure_database(self, conn: sqlite3.Connection, config: Dict[str, Any]) -> None:
        """Configure SQLite database settings."""
        cursor = conn.cursor()
        
        # Set journal mode
        journal_mode = config.get('journal_mode', 'WAL')
        cursor.execute(f"PRAGMA journal_mode = {journal_mode}")
        
        # Set synchronous mode
        synchronous = config.get('synchronous', 'NORMAL')
        cursor.execute(f"PRAGMA synchronous = {synchronous}")
        
        # Enable foreign keys if requested
        if config.get('foreign_keys', True):
            cursor.execute("PRAGMA foreign_keys = ON")
        
        # Set page size for performance
        cursor.execute("PRAGMA page_size = 4096")
        
        # Set cache size (in pages)
        cursor.execute("PRAGMA cache_size = -64000")  # 64MB cache
        
        conn.commit()
    
    def _determine_optimal_schema(self, posts: List[Dict[str, Any]]) -> str:
        """Determine optimal schema mode based on data characteristics."""
        if not posts:
            return 'flat'
        
        # Analyze data complexity
        sample_size = min(100, len(posts))
        nested_fields = set()
        array_fields = set()
        
        for post in posts[:sample_size]:
            for key, value in post.items():
                if isinstance(value, dict):
                    nested_fields.add(key)
                elif isinstance(value, list):
                    array_fields.add(key)
        
        # Decision logic
        if len(nested_fields) > 5 or len(array_fields) > 3:
            return 'normalized'
        elif len(nested_fields) > 0 or len(array_fields) > 0:
            return 'denormalized'
        else:
            return 'flat'
    
    def _create_normalized_schema(self, conn: sqlite3.Connection, 
                                 posts: List[Dict[str, Any]], config: Dict[str, Any]) -> None:
        """Create normalized database schema."""
        cursor = conn.cursor()
        table_prefix = config.get('table_prefix', 'reddit_')
        
        # Main posts table
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_prefix}posts (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                author TEXT,
                subreddit TEXT,
                url TEXT,
                permalink TEXT,
                selftext TEXT,
                is_video BOOLEAN,
                media_url TEXT,
                date_iso TEXT,
                score INTEGER,
                num_comments INTEGER,
                is_nsfw BOOLEAN,
                is_self BOOLEAN,
                domain TEXT,
                post_type TEXT,
                crosspost_parent_id TEXT,
                created_utc REAL,
                edited BOOLEAN,
                locked BOOLEAN,
                archived BOOLEAN,
                spoiler BOOLEAN,
                stickied BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Awards table
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_prefix}awards (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id TEXT NOT NULL,
                name TEXT,
                count INTEGER,
                coin_price INTEGER,
                icon_url TEXT,
                FOREIGN KEY (post_id) REFERENCES {table_prefix}posts (id)
            )
        """)
        
        # Gallery images table
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_prefix}gallery_images (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id TEXT NOT NULL,
                image_url TEXT NOT NULL,
                image_order INTEGER,
                caption TEXT,
                FOREIGN KEY (post_id) REFERENCES {table_prefix}posts (id)
            )
        """)
        
        # Poll options table
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_prefix}poll_options (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id TEXT NOT NULL,
                option_text TEXT NOT NULL,
                vote_count INTEGER,
                option_order INTEGER,
                FOREIGN KEY (post_id) REFERENCES {table_prefix}posts (id)
            )
        """)
        
        # Media metadata table
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_prefix}media (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id TEXT NOT NULL,
                media_type TEXT,
                media_data TEXT,  -- JSON blob
                FOREIGN KEY (post_id) REFERENCES {table_prefix}posts (id)
            )
        """)
        
        # Export metadata table
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_prefix}export_info (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                export_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                format TEXT,
                schema_version TEXT,
                post_count INTEGER,
                configuration TEXT  -- JSON blob
            )
        """)
        
        conn.commit()
    
    def _create_denormalized_schema(self, conn: sqlite3.Connection,
                                   posts: List[Dict[str, Any]], config: Dict[str, Any]) -> None:
        """Create denormalized database schema."""
        cursor = conn.cursor()
        table_prefix = config.get('table_prefix', 'reddit_')
        
        # Single posts table with JSON columns for complex data
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_prefix}posts (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                author TEXT,
                subreddit TEXT,
                url TEXT,
                permalink TEXT,
                selftext TEXT,
                is_video BOOLEAN,
                media_url TEXT,
                date_iso TEXT,
                score INTEGER,
                num_comments INTEGER,
                is_nsfw BOOLEAN,
                is_self BOOLEAN,
                domain TEXT,
                post_type TEXT,
                crosspost_parent_id TEXT,
                created_utc REAL,
                edited BOOLEAN,
                locked BOOLEAN,
                archived BOOLEAN,
                spoiler BOOLEAN,
                stickied BOOLEAN,
                awards_json TEXT,  -- JSON array
                gallery_images_json TEXT,  -- JSON array
                poll_data_json TEXT,  -- JSON object
                media_json TEXT,  -- JSON object
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Export metadata table
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_prefix}export_info (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                export_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                format TEXT,
                schema_version TEXT,
                post_count INTEGER,
                configuration TEXT  -- JSON blob
            )
        """)
        
        conn.commit()
    
    def _create_flat_schema(self, conn: sqlite3.Connection,
                           posts: List[Dict[str, Any]], config: Dict[str, Any]) -> None:
        """Create flat database schema with all fields as columns."""
        cursor = conn.cursor()
        table_prefix = config.get('table_prefix', 'reddit_')
        
        # Analyze all fields in posts to create comprehensive schema
        all_fields = set()
        for post in posts[:100]:  # Sample first 100 posts
            all_fields.update(post.keys())
        
        # Build CREATE TABLE statement dynamically
        columns = ['id TEXT PRIMARY KEY']
        
        # Map field types
        field_types = self._analyze_field_types(posts, all_fields)
        
        for field in sorted(all_fields):
            if field != 'id':  # Already added as primary key
                field_type = field_types.get(field, 'TEXT')
                columns.append(f"{field} {field_type}")
        
        columns.append('created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
        
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_prefix}posts (
                {', '.join(columns)}
            )
        """
        
        cursor.execute(create_sql)
        
        # Export metadata table
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_prefix}export_info (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                export_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                format TEXT,
                schema_version TEXT,
                post_count INTEGER,
                configuration TEXT  -- JSON blob
            )
        """)
        
        conn.commit()
    
    def _analyze_field_types(self, posts: List[Dict[str, Any]], fields: Set[str]) -> Dict[str, str]:
        """Analyze field types from post data."""
        field_types = {}
        sample_size = min(100, len(posts))
        
        for field in fields:
            type_counts = {'TEXT': 0, 'INTEGER': 0, 'REAL': 0, 'BOOLEAN': 0}
            
            for post in posts[:sample_size]:
                value = post.get(field)
                if value is not None:
                    if isinstance(value, bool):
                        type_counts['BOOLEAN'] += 1
                    elif isinstance(value, int):
                        type_counts['INTEGER'] += 1
                    elif isinstance(value, float):
                        type_counts['REAL'] += 1
                    else:
                        type_counts['TEXT'] += 1
            
            # Choose most common type
            field_types[field] = max(type_counts, key=type_counts.get)
        
        return field_types
    
    def _insert_normalized_data(self, conn: sqlite3.Connection, posts: List[Dict[str, Any]],
                               data: Dict[str, Any], config: Dict[str, Any], 
                               result: ExportResult) -> None:
        """Insert data into normalized schema."""
        cursor = conn.cursor()
        table_prefix = config.get('table_prefix', 'reddit_')
        batch_size = config.get('batch_size', 1000)
        
        # Insert posts in batches
        for i in range(0, len(posts), batch_size):
            batch = posts[i:i + batch_size]
            
            # Insert main post data
            post_rows = []
            for post in batch:
                row = self._extract_main_post_fields(post, config)
                post_rows.append(row)
            
            cursor.executemany(f"""
                INSERT OR REPLACE INTO {table_prefix}posts 
                (id, title, author, subreddit, url, permalink, selftext, is_video,
                 media_url, date_iso, score, num_comments, is_nsfw, is_self, domain,
                 post_type, crosspost_parent_id, created_utc, edited, locked, 
                 archived, spoiler, stickied)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, post_rows)
            
            # Insert related data
            for post in batch:
                post_id = post.get('id')
                if not post_id:
                    continue
                
                # Insert awards
                awards = post.get('awards', [])
                if awards and isinstance(awards, list):
                    for award in awards:
                        if isinstance(award, dict):
                            cursor.execute(f"""
                                INSERT INTO {table_prefix}awards
                                (post_id, name, count, coin_price, icon_url)
                                VALUES (?, ?, ?, ?, ?)
                            """, (post_id, award.get('name'), award.get('count'),
                                 award.get('coin_price'), award.get('icon_url')))
                
                # Insert gallery images
                gallery_urls = post.get('gallery_image_urls', [])
                if gallery_urls and isinstance(gallery_urls, list):
                    for i, url in enumerate(gallery_urls):
                        cursor.execute(f"""
                            INSERT INTO {table_prefix}gallery_images
                            (post_id, image_url, image_order)
                            VALUES (?, ?, ?)
                        """, (post_id, url, i))
                
                # Insert poll data
                poll_data = post.get('poll_data')
                if poll_data and isinstance(poll_data, dict):
                    options = poll_data.get('options', [])
                    for i, option in enumerate(options):
                        if isinstance(option, dict):
                            cursor.execute(f"""
                                INSERT INTO {table_prefix}poll_options
                                (post_id, option_text, vote_count, option_order)
                                VALUES (?, ?, ?, ?)
                            """, (post_id, option.get('text'), 
                                 option.get('vote_count'), i))
                
                # Insert media data
                media = post.get('media')
                if media:
                    media_json = json.dumps(media) if isinstance(media, dict) else str(media)
                    cursor.execute(f"""
                        INSERT INTO {table_prefix}media
                        (post_id, media_type, media_data)
                        VALUES (?, ?, ?)
                    """, (post_id, 'reddit_media', media_json))
            
            conn.commit()
        
        # Insert export metadata
        self._insert_export_metadata(conn, data, config, len(posts))
    
    def _insert_denormalized_data(self, conn: sqlite3.Connection, posts: List[Dict[str, Any]],
                                 data: Dict[str, Any], config: Dict[str, Any],
                                 result: ExportResult) -> None:
        """Insert data into denormalized schema."""
        cursor = conn.cursor()
        table_prefix = config.get('table_prefix', 'reddit_')
        batch_size = config.get('batch_size', 1000)
        
        # Insert posts in batches
        for i in range(0, len(posts), batch_size):
            batch = posts[i:i + batch_size]
            post_rows = []
            
            for post in batch:
                row = self._extract_main_post_fields(post, config)
                
                # Add JSON fields
                awards_json = json.dumps(post.get('awards', [])) if post.get('awards') else None
                gallery_json = json.dumps(post.get('gallery_image_urls', [])) if post.get('gallery_image_urls') else None
                poll_json = json.dumps(post.get('poll_data')) if post.get('poll_data') else None
                media_json = json.dumps(post.get('media')) if post.get('media') else None
                
                row.extend([awards_json, gallery_json, poll_json, media_json])
                post_rows.append(row)
            
            cursor.executemany(f"""
                INSERT OR REPLACE INTO {table_prefix}posts 
                (id, title, author, subreddit, url, permalink, selftext, is_video,
                 media_url, date_iso, score, num_comments, is_nsfw, is_self, domain,
                 post_type, crosspost_parent_id, created_utc, edited, locked, 
                 archived, spoiler, stickied, awards_json, gallery_images_json,
                 poll_data_json, media_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, post_rows)
            
            conn.commit()
        
        # Insert export metadata
        self._insert_export_metadata(conn, data, config, len(posts))
    
    def _insert_flat_data(self, conn: sqlite3.Connection, posts: List[Dict[str, Any]],
                         data: Dict[str, Any], config: Dict[str, Any],
                         result: ExportResult) -> None:
        """Insert data into flat schema."""
        cursor = conn.cursor()
        table_prefix = config.get('table_prefix', 'reddit_')
        batch_size = config.get('batch_size', 1000)
        
        # Get all columns from table
        cursor.execute(f"PRAGMA table_info({table_prefix}posts)")
        columns = [row[1] for row in cursor.fetchall() if row[1] != 'created_at']
        
        # Insert posts in batches
        for i in range(0, len(posts), batch_size):
            batch = posts[i:i + batch_size]
            post_rows = []
            
            for post in batch:
                row = []
                for column in columns:
                    value = post.get(column)
                    
                    # Convert complex types to JSON strings
                    if isinstance(value, (dict, list)):
                        value = json.dumps(value)
                    elif value is None:
                        value = None
                    else:
                        value = str(value)
                    
                    row.append(value)
                
                post_rows.append(row)
            
            placeholders = ', '.join(['?' for _ in columns])
            column_list = ', '.join(columns)
            
            cursor.executemany(f"""
                INSERT OR REPLACE INTO {table_prefix}posts ({column_list})
                VALUES ({placeholders})
            """, post_rows)
            
            conn.commit()
        
        # Insert export metadata
        self._insert_export_metadata(conn, data, config, len(posts))
    
    def _extract_main_post_fields(self, post: Dict[str, Any], config: Dict[str, Any]) -> List[Any]:
        """Extract main post fields for database insertion."""
        date_format = config.get('date_format', 'iso')
        
        # Convert created_utc to desired format
        created_utc = post.get('created_utc', 0)
        if isinstance(created_utc, (int, float)) and created_utc > 0:
            if date_format == 'timestamp':
                date_value = created_utc
            else:  # iso or readable
                try:
                    dt = datetime.fromtimestamp(created_utc)
                    date_value = dt.isoformat() if date_format == 'iso' else dt.strftime('%Y-%m-%d %H:%M:%S')
                except (ValueError, OSError):
                    date_value = created_utc
        else:
            date_value = created_utc
        
        return [
            post.get('id', ''),
            post.get('title', ''),
            post.get('author', ''),
            post.get('subreddit', ''),
            post.get('url', ''),
            post.get('permalink', ''),
            post.get('selftext', ''),
            bool(post.get('is_video', False)),
            post.get('media_url'),
            post.get('date_iso', ''),
            int(post.get('score', 0)),
            int(post.get('num_comments', 0)),
            bool(post.get('is_nsfw', False)),
            bool(post.get('is_self', False)),
            post.get('domain', ''),
            post.get('post_type', 'link'),
            post.get('crosspost_parent_id'),
            float(created_utc) if created_utc else None,
            bool(post.get('edited', False)),
            bool(post.get('locked', False)),
            bool(post.get('archived', False)),
            bool(post.get('spoiler', False)),
            bool(post.get('stickied', False))
        ]
    
    def _insert_export_metadata(self, conn: sqlite3.Connection, data: Dict[str, Any],
                               config: Dict[str, Any], post_count: int) -> None:
        """Insert export metadata into database."""
        cursor = conn.cursor()
        table_prefix = config.get('table_prefix', 'reddit_')
        
        cursor.execute(f"""
            INSERT INTO {table_prefix}export_info
            (format, schema_version, post_count, configuration)
            VALUES (?, ?, ?, ?)
        """, ('sqlite', '2.0', post_count, json.dumps(config)))
        
        conn.commit()
    
    def _create_indexes(self, conn: sqlite3.Connection, config: Dict[str, Any]) -> None:
        """Create performance indexes."""
        cursor = conn.cursor()
        table_prefix = config.get('table_prefix', 'reddit_')
        
        # Common indexes for posts table
        indexes = [
            f"CREATE INDEX IF NOT EXISTS idx_{table_prefix}posts_subreddit ON {table_prefix}posts (subreddit)",
            f"CREATE INDEX IF NOT EXISTS idx_{table_prefix}posts_author ON {table_prefix}posts (author)",
            f"CREATE INDEX IF NOT EXISTS idx_{table_prefix}posts_score ON {table_prefix}posts (score)",
            f"CREATE INDEX IF NOT EXISTS idx_{table_prefix}posts_created_utc ON {table_prefix}posts (created_utc)",
            f"CREATE INDEX IF NOT EXISTS idx_{table_prefix}posts_post_type ON {table_prefix}posts (post_type)",
            f"CREATE INDEX IF NOT EXISTS idx_{table_prefix}posts_nsfw ON {table_prefix}posts (is_nsfw)"
        ]
        
        for index_sql in indexes:
            try:
                cursor.execute(index_sql)
            except sqlite3.Error as e:
                self.logger.warning(f"Failed to create index: {e}")
        
        conn.commit()
    
    def _create_fts_tables(self, conn: sqlite3.Connection, config: Dict[str, Any]) -> None:
        """Create full-text search tables."""
        cursor = conn.cursor()
        table_prefix = config.get('table_prefix', 'reddit_')
        
        try:
            # Create FTS table for post content
            cursor.execute(f"""
                CREATE VIRTUAL TABLE IF NOT EXISTS {table_prefix}posts_fts 
                USING fts5(id, title, selftext, author, subreddit, 
                          content='{table_prefix}posts', content_rowid='rowid')
            """)
            
            # Populate FTS table
            cursor.execute(f"""
                INSERT INTO {table_prefix}posts_fts(id, title, selftext, author, subreddit)
                SELECT id, title, selftext, author, subreddit FROM {table_prefix}posts
            """)
            
            conn.commit()
            
        except sqlite3.Error as e:
            self.logger.warning(f"Failed to create FTS tables: {e}")
    
    def _analyze_database(self, conn: sqlite3.Connection) -> None:
        """Analyze database for query optimization."""
        cursor = conn.cursor()
        try:
            cursor.execute("ANALYZE")
            conn.commit()
        except sqlite3.Error as e:
            self.logger.warning(f"Failed to analyze database: {e}")
    
    def _vacuum_database(self, conn: sqlite3.Connection) -> None:
        """Vacuum database to optimize storage."""
        try:
            conn.execute("VACUUM")
        except sqlite3.Error as e:
            self.logger.warning(f"Failed to vacuum database: {e}")
    
    def _backup_database(self, database_path: Path) -> None:
        """Create backup of existing database."""
        if not database_path.exists():
            return
        
        backup_path = database_path.with_suffix(
            f"{database_path.suffix}.backup.{int(time.time())}"
        )
        
        try:
            import shutil
            shutil.copy2(database_path, backup_path)
            self.logger.info(f"Created database backup: {backup_path}")
        except Exception as e:
            self.logger.warning(f"Failed to create database backup: {e}")
    
    def _create_empty_database(self, output_file: Path, config: Dict[str, Any]) -> None:
        """Create empty database with schema."""
        with sqlite3.connect(str(output_file)) as conn:
            self._configure_database(conn, config)
            
            schema_mode = config.get('schema_mode', 'flat')
            if schema_mode == 'normalized':
                self._create_normalized_schema(conn, [], config)
            elif schema_mode == 'denormalized':
                self._create_denormalized_schema(conn, [], config)
            else:
                self._create_flat_schema(conn, [], config)
    
    def _get_size_factor(self) -> float:
        """Get size factor for SQLite format."""
        return 1.2  # SQLite has some overhead but good compression