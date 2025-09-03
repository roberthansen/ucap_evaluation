import sys
import yaml
from pathlib import Path

sys.path=[str(Path().cwd())] + sys.path
from src.curtailment_report_downloader.curtailment_report_downloader \
    import CurtailmentReportDownloader

if __name__=='__main__':
    with open('config/config.yaml','r') as f:
        config = yaml.safe_load(f)
    
    curtailment_report_downloader = CurtailmentReportDownloader(config)
    curtailment_report_downloader.download_all_reports()
    curtailment_report_downloader.update_parquet()