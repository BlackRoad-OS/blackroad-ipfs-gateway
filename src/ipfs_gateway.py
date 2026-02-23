#!/usr/bin/env python3
"""
IPFS Gateway and Content Addressing System
Provides local IPFS-like functionality with SHA-256 based content addressing
"""

import os
import json
import sqlite3
import hashlib
import shutil
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any
import argparse


STORE_DIR = Path.home() / ".blackroad" / "ipfs-store"
DB_PATH = Path.home() / ".blackroad" / "ipfs.db"


@dataclass
class ContentObject:
    """Represents a content object in IPFS"""
    cid: str
    name: str
    size_bytes: int
    mime_type: str
    pinned: bool
    uploaded_at: str
    local_path: str


class IPFSGateway:
    """IPFS Gateway implementation with local storage"""
    
    def __init__(self):
        """Initialize gateway with storage directories and database"""
        STORE_DIR.mkdir(parents=True, exist_ok=True)
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
    
    def _init_db(self):
        """Initialize SQLite database schema"""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS objects (
                cid TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                size_bytes INTEGER,
                mime_type TEXT,
                pinned BOOLEAN DEFAULT 0,
                uploaded_at TEXT,
                local_path TEXT UNIQUE
            )
        ''')
        conn.commit()
        conn.close()
    
    def _compute_cid(self, data: bytes) -> str:
        """Compute SHA-256 based mock CID"""
        hash_obj = hashlib.sha256(data)
        return f"Qm{hash_obj.hexdigest()[:44]}"
    
    def add_file(self, path: str) -> ContentObject:
        """Add file to gateway, compute CID and store metadata"""
        file_path = Path(path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {path}")
        
        # Read file and compute CID
        with open(file_path, 'rb') as f:
            data = f.read()
        cid = self._compute_cid(data)
        
        # Determine MIME type
        mime_type = "application/octet-stream"
        if file_path.suffix == '.json':
            mime_type = "application/json"
        elif file_path.suffix == '.txt':
            mime_type = "text/plain"
        elif file_path.suffix == '.md':
            mime_type = "text/markdown"
        
        # Store file
        local_path = STORE_DIR / cid
        shutil.copy2(file_path, local_path)
        
        # Create content object
        obj = ContentObject(
            cid=cid,
            name=file_path.name,
            size_bytes=len(data),
            mime_type=mime_type,
            pinned=False,
            uploaded_at=datetime.now().isoformat(),
            local_path=str(local_path)
        )
        
        # Store metadata
        self._store_metadata(obj)
        return obj
    
    def add_json(self, data: Dict[Any, Any], name: str = "data.json") -> ContentObject:
        """Serialize dict and add as JSON"""
        json_str = json.dumps(data, indent=2)
        json_bytes = json_str.encode('utf-8')
        cid = self._compute_cid(json_bytes)
        
        local_path = STORE_DIR / cid
        with open(local_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        obj = ContentObject(
            cid=cid,
            name=name,
            size_bytes=len(json_bytes),
            mime_type="application/json",
            pinned=False,
            uploaded_at=datetime.now().isoformat(),
            local_path=str(local_path)
        )
        
        self._store_metadata(obj)
        return obj
    
    def get(self, cid: str) -> Optional[ContentObject]:
        """Retrieve content by CID from local cache"""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM objects WHERE cid = ?', (cid,))
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            return None
        
        return ContentObject(
            cid=row[0], name=row[1], size_bytes=row[2],
            mime_type=row[3], pinned=bool(row[4]),
            uploaded_at=row[5], local_path=row[6]
        )
    
    def pin(self, cid: str) -> bool:
        """Mark content as pinned (won't be garbage collected)"""
        obj = self.get(cid)
        if not obj:
            return False
        
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('UPDATE objects SET pinned = 1 WHERE cid = ?', (cid,))
        conn.commit()
        conn.close()
        return True
    
    def unpin(self, cid: str) -> bool:
        """Remove pin from content"""
        obj = self.get(cid)
        if not obj:
            return False
        
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('UPDATE objects SET pinned = 0 WHERE cid = ?', (cid,))
        conn.commit()
        conn.close()
        return True
    
    def gc(self):
        """Remove unpinned objects older than 24 hours"""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cutoff_time = (datetime.now() - timedelta(hours=24)).isoformat()
        cursor.execute(
            'SELECT cid, local_path FROM objects WHERE pinned = 0 AND uploaded_at < ?',
            (cutoff_time,)
        )
        rows = cursor.fetchall()
        
        for cid, local_path in rows:
            try:
                Path(local_path).unlink()
                cursor.execute('DELETE FROM objects WHERE cid = ?', (cid,))
            except Exception as e:
                print(f"Error deleting {cid}: {e}")
        
        conn.commit()
        conn.close()
        return len(rows)
    
    def ls(self, cid: Optional[str] = None) -> List[ContentObject]:
        """List objects or objects inside a directory CID"""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        if cid:
            cursor.execute('SELECT * FROM objects WHERE cid = ?', (cid,))
        else:
            cursor.execute('SELECT * FROM objects')
        
        rows = cursor.fetchall()
        conn.close()
        
        return [
            ContentObject(
                cid=row[0], name=row[1], size_bytes=row[2],
                mime_type=row[3], pinned=bool(row[4]),
                uploaded_at=row[5], local_path=row[6]
            )
            for row in rows
        ]
    
    def stat(self, cid: str) -> Optional[Dict[str, Any]]:
        """Get size, block count, and mime type"""
        obj = self.get(cid)
        if not obj:
            return None
        
        return {
            'cid': cid,
            'name': obj.name,
            'size_bytes': obj.size_bytes,
            'mime_type': obj.mime_type,
            'pinned': obj.pinned,
            'uploaded_at': obj.uploaded_at
        }
    
    def proxy_url(self, cid: str, gateway: str = "https://ipfs.io") -> str:
        """Return public gateway URL"""
        return f"{gateway}/ipfs/{cid}"
    
    def export_car(self, cids: List[str], output_path: str):
        """Export objects as CAR-like JSON format"""
        objects = []
        for cid in cids:
            obj = self.get(cid)
            if obj:
                objects.append(asdict(obj))
        
        with open(output_path, 'w') as f:
            json.dump({'version': 1, 'objects': objects}, f, indent=2)
    
    def _store_metadata(self, obj: ContentObject):
        """Store metadata in database"""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO objects 
            (cid, name, size_bytes, mime_type, pinned, uploaded_at, local_path)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (obj.cid, obj.name, obj.size_bytes, obj.mime_type, obj.pinned,
              obj.uploaded_at, obj.local_path))
        conn.commit()
        conn.close()


def main():
    """CLI interface"""
    parser = argparse.ArgumentParser(description="IPFS Gateway")
    subparsers = parser.add_subparsers(dest='command')
    
    # Add command
    add_parser = subparsers.add_parser('add', help='Add file to gateway')
    add_parser.add_argument('path', help='File path to add')
    
    # List command
    ls_parser = subparsers.add_parser('ls', help='List objects')
    ls_parser.add_argument('cid', nargs='?', help='Optional CID to list')
    
    # Get command
    get_parser = subparsers.add_parser('get', help='Get object by CID')
    get_parser.add_argument('cid', help='CID to retrieve')
    
    # Stat command
    stat_parser = subparsers.add_parser('stat', help='Get object stats')
    stat_parser.add_argument('cid', help='CID to stat')
    
    # Pin command
    pin_parser = subparsers.add_parser('pin', help='Pin object')
    pin_parser.add_argument('cid', help='CID to pin')
    
    # GC command
    gc_parser = subparsers.add_parser('gc', help='Garbage collect')
    
    args = parser.parse_args()
    gateway = IPFSGateway()
    
    if args.command == 'add':
        obj = gateway.add_file(args.path)
        print(f"Added: {obj.cid} ({obj.name})")
    elif args.command == 'ls':
        objects = gateway.ls(args.cid)
        for obj in objects:
            print(f"{obj.cid} {obj.name} ({obj.size_bytes} bytes)")
    elif args.command == 'get':
        obj = gateway.get(args.cid)
        if obj:
            print(json.dumps(asdict(obj), indent=2))
        else:
            print("Not found")
    elif args.command == 'stat':
        stat = gateway.stat(args.cid)
        if stat:
            print(json.dumps(stat, indent=2))
        else:
            print("Not found")
    elif args.command == 'pin':
        if gateway.pin(args.cid):
            print(f"Pinned: {args.cid}")
        else:
            print("Not found")
    elif args.command == 'gc':
        count = gateway.gc()
        print(f"Garbage collected {count} objects")


if __name__ == '__main__':
    main()
