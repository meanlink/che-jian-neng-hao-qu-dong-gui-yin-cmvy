from enum import Enum
class Role(str, Enum):
    admin = 'admin'
    analyst = 'analyst'
    viewer = 'viewer'
class JobStatus(str, Enum):
    queued = 'queued'
    running = 'running'
    completed = 'completed'
    failed = 'failed'
class BackupStatus(str, Enum):
    ready = 'ready'
    verifying = 'verifying'
    restoring = 'restoring'
    restored = 'restored'
    failed = 'failed'
