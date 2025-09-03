# UCAP Evaluation
This package is intended to help users collect resource curtailment data from CAISO's website, and analyze the data to determine resource-specific Unforced Capacity (UCAP) values for use in CPUC's Resource Adequacy proceeding, R.23-10-011.

The package includes scripts written in Python, and requires following standard libraries; although the package was tested with the listed versions, other versions may work:
- openpyxl 3.0.9
- numpy 1.22.3
- pandas 1.3.4
- pyarrow 4.0.1
- pyyaml 6.0

Configuration files are written in yaml and should be updated to reflect the user's environment, including the desired directories to which various downloads and output files should be saved.

This package was developed in Windows 11, but should work on any operating system after adjusting the config.yaml file to reflect the directory structure.