import sys
import yaml
import random
import unittest
from pathlib import Path
from datetime import datetime as dt, date as d, timedelta as td

sys.path=['M:\\Users\\RH2\\src\\ucap_evaluation'] + sys.path
from src.curtailment_report_downloader.curtailment_report_downloader \
    import CurtailmentReportDownloader

class TestCurtailmentReportDownloader(unittest.TestCase):

    config_path = Path(r'M:\Users\RH2\src\ucap_evaluation\config\config.yaml')

    def __init__(self,*args,**kwargs):
        # Modifies the class initializer to incorporate configuration settings.
        super(TestCurtailmentReportDownloader,self).__init__(*args,**kwargs)
        with self.config_path.open('r') as f:
            self.config = yaml.safe_load(f)
        self.curtailment_report_downloader = CurtailmentReportDownloader(self.config)

        # Generate a set of dates representing each standard and exception rule
        # in the configuration settings.
        first_date = self.config['caiso_curtailment_reports']['first_available_date']
        standard_dates = [ \
            first_date + td(days=n) for n in \
            range((d.today()-first_date).days+1) \
        ]
        standard_template = self.config['caiso_curtailment_reports']['url']['standard']
        test_categories = []
        test_dates = []
        test_templates = []
        exception_number = 0
        for exc in self.config['caiso_curtailment_reports']['url']['exceptions']:
            exception_number+=1
            if exc['type']=='range':
                exception_dates= [ \
                    exc['dates'][0] + td(days=n) for n in \
                    range((exc['dates'][1]-exc['dates'][0]).days + 1) \
                ]
                standard_dates = list(filter( \
                    lambda x:x not in exception_dates, \
                    standard_dates \
                ))
                test_categories.append(f'Exception {exception_number} - Date Range')
                test_dates.append(random.choice(exception_dates))
                test_templates.append(exc['template'])
            elif exc['type']=='list':
                exception_dates = exc['dates']
                standard_dates = list(filter( \
                    lambda x:x not in exception_dates, \
                    standard_dates \
                ))
                test_categories.append(f'Exception {exception_number} - Date List')
                test_dates.append(random.choice(exception_dates))
                test_templates.append(exc['template'])
            else:
                pass
        test_categories.append('Standard')
        test_dates.append(random.choice(standard_dates))
        test_templates.append(standard_template)
        self.test_categories = test_categories
        self.test_dates = test_dates
        self.test_templates = test_templates

    def test_curtailment_report_urls(self):
        # Test the outputs of the curtailment report url method against the
        # configuration settings.
        for category,date,template in zip(self.test_categories,self.test_dates,self.test_templates):
            url = self.curtailment_report_downloader.url_by_date(date)
            print('URL Test - ' + category)
            print('\tTest Date:\t' + date.strftime('%Y-%m-%d'))
            print('\tTemplate:\t' + template)
            print('\tURL:\t\t' + url)
            print('\tParsed Date:\t' + dt.strptime(url,template).strftime('%Y-%m-%d'))
            if dt.strptime(url,template).date()==date:
                print('\tPASS: URL matches applicable template')
            else:
                print('\tFAIL: URL does not match applicable template!')

    def test_curtailment_report_download_path(self):
        template = self.config['caiso_curtailment_reports']['download_path_template']
        for date in self.test_dates:
            path = self.curtailment_report_downloader.download_path_by_date(date)
            print('Download Path Test')
            print('\tTest Date:\t' + date.strftime('%Y-%m-%d'))
            print('\tTemplate:\t' + template)
            print(f'\tDownload Path:\t{path}')
            print('\tParsed Date:\t' + dt.strptime(str(path),template).strftime('%Y-%m-%d'))
            if dt.strptime(str(path),template).date()==date:
                print('\tPASS: Download path matches template')
            else:
                print('\tFAIL: Download path does not match template')

    def test_curtailment_report_download_single(self):
        download_count = 0
        for date in self.test_dates:
            path = self.curtailment_report_downloader.download_path_by_date(date)
            result = self.curtailment_report_downloader.download_report_by_date(date)
            if result==200:
                download_count += 1
        if download_count==len(self.test_dates):
            if download_count>1:
                print(f'Successfully downloaded {download_count} files')
            elif download_count>0:
                print('Successfully downloaded 1 file')
            else:
                print('No downloads')
        self.curtailment_report_downloader.clear_all_downloads()

if __name__=='__main__':
    unittest.main()