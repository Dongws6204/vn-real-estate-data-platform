import os
import glob
import logging
from datetime import datetime

# Configure logging for the cleanup script itself
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def clean_logs(log_dir: str = 'logs', backup: bool = True) -> None:
    """
    Clean all log files in the specified directory.
    
    Args:
        log_dir (str): Directory containing log files
        backup (bool): If True, create backup before cleaning
    """
    try:
        # Check if logs directory exists
        if not os.path.exists(log_dir):
            logger.info(f"Log directory '{log_dir}' does not exist. Nothing to clean.")
            return

        # Create backup if requested
        if backup:
            backup_dir = create_backup(log_dir)
            if backup_dir:
                logger.info(f"Created backup in: {backup_dir}")

        # Find all log files
        log_patterns = [
            '*.log',
            '*.log.*',  # For rotated logs like mongodb.log.1, mongodb.log.2, etc.
        ]

        files_removed = 0
        bytes_freed = 0

        for pattern in log_patterns:
            log_files = glob.glob(os.path.join(log_dir, pattern))
            
            for log_file in log_files:
                try:
                    # Get file size before removal
                    file_size = os.path.getsize(log_file)
                    
                    # Remove the file
                    os.remove(log_file)
                    
                    files_removed += 1
                    bytes_freed += file_size
                    
                    logger.info(f"Removed: {log_file}")
                except Exception as e:
                    logger.error(f"Error removing {log_file}: {str(e)}")

        # Convert bytes to MB for readable output
        mb_freed = bytes_freed / (1024 * 1024)
        
        logger.info(f"Cleanup completed:")
        logger.info(f"- Files removed: {files_removed}")
        logger.info(f"- Space freed: {mb_freed:.2f} MB")

    except Exception as e:
        logger.error(f"Error during cleanup: {str(e)}")
        raise

def create_backup(log_dir: str) -> str:
    """
    Create a backup of the logs directory.
    
    Args:
        log_dir (str): Directory to backup
        
    Returns:
        str: Path to backup directory
    """
    try:
        # Create backups directory if it doesn't exist
        backup_base = 'logs_backup'
        if not os.path.exists(backup_base):
            os.makedirs(backup_base)

        # Create timestamp for backup directory name
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_dir = os.path.join(backup_base, f'logs_backup_{timestamp}')
        
        # Create backup directory
        os.makedirs(backup_dir)
        
        # Copy all files
        log_files = glob.glob(os.path.join(log_dir, '*.*'))
        for file_path in log_files:
            if os.path.isfile(file_path):
                file_name = os.path.basename(file_path)
                backup_path = os.path.join(backup_dir, file_name)
                
                # Read and write file content
                with open(file_path, 'rb') as src, open(backup_path, 'wb') as dst:
                    dst.write(src.read())
                
                logger.info(f"Backed up: {file_name}")
        
        return backup_dir
    
    except Exception as e:
        logger.error(f"Error creating backup: {str(e)}")
        return ''

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Clean log files')
    parser.add_argument('--no-backup', action='store_true', help='Skip creating backup before cleaning')
    parser.add_argument('--log-dir', default='logs', help='Directory containing log files')
    
    args = parser.parse_args()
    
    clean_logs(
        log_dir=args.log_dir,
        backup=not args.no_backup
    )