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

# UCAP Definition
UCAP means "Unforced Capacity". It is a measure of the expected capacity available to the grid, accounting for forced outages. There are many technical nuances and design decisions within this definition that must be considered in order to implement a UCAP framework. Some outstanding and resolved issues are presented below.

## Resource Grouping
Every resource will have its own UCAP value
We have not concluded whether individual units will be assigned UCAP values based solely on their own forced outage rates, or on the collective outage rate of some group to which they belong. Furthermore, we have not yet determined how resources would be grouped. The following options are under consideration:

## All Hours vs. Demand Hours
We have determined the UCAP framework will apply an equivalent forced outage rate evaluated during demand hours (EFORd), as opposed to considering all hours within each evaluation period. However, the definition of demand hours is open to interpretation.

## Selection of Demand Hours
We base our use of demand hours on the definition of EFORd in the GADS Manual, which notes that there are numerous ways to determine demand hours. We further don't have clear insight into how demand hours are determined in GADS itself. We observe three potential approaches broadly, each with additional considerations:
- Individual Resource
    - Based on grid synchronization
    - Based on economic bid data
    - Based on comparison of Pnode and Default Energy Bid
- Grouped Resource
    - Similar metrics as individual, but in aggregate
- Grid-Level
    - Supply Cushion
    - At-Risk Hours
    - Availability Assessment Hours

Our current plan is to implement a grid-level evaluation of demand hours matching CAISO's Availability Assessment Hours, but this is a topic of active discussion. Previously, we had considered adopting CAISO's proposed Supply Cushion approach, but they have since encountered significant stakeholder pushback and are looking into alternatives. As the primary benefit of adopting Supply Cushion hours was alignment between CAISO and CPUC, we no longer believe this is the best solution.

## Evaluation Periods
Forced outage rates can be evaluated for any period of time. Common metrics include monthly, seasonal, and annual. The proposed UCAP framework will evaluate UCAP on a seasonal basis, with two seasons as defined below:
- Summer&mdash;May through October
- Non-Summer&mdash;January through April, November, and December

The seasons will be evaluated within calendar years.

## Data Sources
Energy Division has decided to rely on CAISO's Outage Management System (OMS) presented in the Prior Trade-Day Curtailment Reports as the source-of-truth for resource outages. Prior to implementing a UCAP framework, Energy Division has largely depended on the National Electric Reliability Corporation's (NERC's) Generator Availability Database System (GADS) for similar data. While GADS remains a valuable data source, CAISO's OMS has some key advantages that factored into the decision to use it for UCAP.

First, the Prior Trade-Day Curtailment Reports are available to anyone through CAISO's website. This helps provide transparency into the UCAP evaluation process, as generators will be able to evaluate their own UCAP values and check Energy Division's calculations. 

Second, resources that report into OMS include those that will be subject to UCAP. Conversely, generator units in GADS do not correspond perfectly with the resources as reported to CAISO&mdash;GADS may distinguish between multiple generators, steam, and natural gas turbines that are represented as a single resource in OMS, for instance. This adds a burden on Energy Division staff to maintain a mapping between GADS units and CAISO resources, and introduces complications of aggregating or disaggregating outage rates.

Third, NERC has not yet (as of Summer 2025) incorporated outage data from Battery Electric Storage Systems (BESS) into GADS, while BESS resources do report into OMS and appear in the Prior Trade-Day Curtailment Reports.

It is important to acknowledge the considerable disadvantages to using OMS rather than GADS, including inconsistencies in reporting standards across generators and outstanding questions about comparing outage events between GADS and OMS, but we have determined that the benefits outweight these concerns.

## Outage Types and Nature-of-Work Codes
Resource outages in OMS are flagged with two required parameters: an outage type, and a nature-of-work. The following table present all combinations of these two flags present in outage data from 2022-2024. CAISO plans to expand the list of possible entries, and there is some flexibility in how resource operators report their outages, but we hope to determine a consistent subset of these codes to use in the UCAP evaluation. The codes we currently plan to include are denoted in the table, but we are actively working with CAISO to refine this selection. 

| OUTAGE TYPE | NATURE OF WORK                            | Include in UCAP |
| :---:       | :---                                      | :---:           |
| FORCED      | AMBIENT_DUE_TO_FUEL_INSUFFICIENCY         | No              |
| FORCED      | AMBIENT_DUE_TO_TEMP                       | Yes[^1]         |
| FORCED      | AMBIENT_NOT_DUE_TO_TEMP                   | No              |
| FORCED      | ANNUAL_USE_LIMIT_REACHED                  | No              |
| FORCED      | AVR_EXITER                                | Yes[^2]         |
| FORCED      | ENVIRONMENTAL_RESTRICTIONS                | Yes             |
| FORCED      | ICCP                                      | Yes             |
| FORCED      | METERING_TELEMETRY                        | Yes             |
| FORCED      | MONTHLY_USE_LIMIT_REACHED                 | No              |
| FORCED      | NEW_GENERATOR_TEST_ENERGY                 | No              |
| FORCED      | OTHER_USE_LIMIT_REACHED                   | No              |
| FORCED      | PLANT_MAINTENANCE                         | Yes             |
| FORCED      | PLANT_TROUBLE                             | Yes             |
| FORCED      | POWER_SYSTEM_STABILIZATION                | Yes[^2]         |
| FORCED      | RIMS_OUTAGE                               | No              |
| FORCED      | RIMS_TESTING                              | No              |
| FORCED      | RTU_RIG                                   | Yes             |
| FORCED      | SHORT_TERM_USE_LIMIT_REACHED              | No              |
| FORCED      | TECHNICAL_LIMITATIONS_NOT_IN_MARKET_MODEL | Yes             |
| FORCED      | TRANSITIONAL_LIMITATION                   | Yes             |
| FORCED      | TRANSMISSION_INDUCED                      | No              |
| FORCED      | UNIT_SUPPORTING_STARTUP                   | No              |
| FORCED      | UNIT_TESTING                              | No              |
| PLANNED     | AMBIENT_DUE_TO_FUEL_INSUFFICIENCY         | No              |
| PLANNED     | AMBIENT_DUE_TO_TEMP                       | No              |
| PLANNED     | AMBIENT_NOT_DUE_TO_TEMP                   | No              |
| PLANNED     | ENVIRONMENTAL_RESTRICTIONS                | No              |
| PLANNED     | METERING_TELEMETRY                        | No              |
| PLANNED     | NEW_GENERATOR_TEST_ENERGY                 | No              |
| PLANNED     | PLANT_MAINTENANCE                         | No              |
| PLANNED     | PLANT_TROUBLE                             | No              |
| PLANNED     | RIMS_OUTAGE                               | No              |
| PLANNED     | RTU_RIG                                   | No              |
| PLANNED     | SHORT_TERM_USE_LIMIT_REACHED              | No              |
| PLANNED     | TECHNICAL_LIMITATIONS_NOT_IN_MARKET_MODEL | No              |
| PLANNED     | TRANSITIONAL_LIMITATION                   | No              |
| PLANNED     | TRANSMISSION_INDUCED                      | No              |
| PLANNED     | UNIT_SUPPORTING_STARTUP                   | No              |
| PLANNED     | UNIT_TESTING                              | No              |

[^1]: Ambient outages due to temperature are treated separately from other forced outages as noted in a later section.

[^2]: This nature-of-work code does not appear in previous years' curtailment reports, but is included for future outages.

## Exceptional Outages
We have signaled to stakeholders that we may allow for some outage events to be discounted or excluded from UCAP evaluation, but have not concluded on the final approach. This issue also affects how the UCAP framework will address major repairs and capitol improvements. Some possibilities are listed below:
- Evaluate the forced outage rates for the past four years, and only include each resource's best three years into its UCAP value.
- Evaluate the forced outage rates for the past three years and apply weights such that more recent years have greater influence on the UCAP values than earlier years.
- Allow resource operators to request specific outage events be excluded from UCAP evaluation and provide an explanation regarding why.
- Automatically identify and exclude extreme outage events with a predefined rubric.

## Handling New Resources
Since we propose to predicate the UCAP framework on historic outage data, new resources pose a clear problem, especially if we elect to evaluate UCAP on the basis of individual resources. At the moment, we plan to address this issue by applying a weighted group-averaged outage rate in place of any missing historic data.

## Ambient Outages due to Temperature
We propose to incorporate an additional deration factor due to ambient temperatures into the UCAP evaluation framework. Forced outages flagged as having been due to ambient temperatures will be excluded from the initial evaluation of forced outage rates, and instead will be used to forecast derations for future years using historic weather data and climate modeling. Outage rates due to ambient temperatures will be calculated in the following general process, with some technical details yet to be determined:
- Determine ambient temperatures coincident with each reported outage.
- Perform a linear regression analysis of outages vs. ambient temperatures to determine percent outage as a function of temperature for each resource.
- Calculate forecast outages using a modeled weather-year for the 