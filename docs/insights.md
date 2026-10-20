# Insights: what the registers say beyond "how many companies, where"

These tables live in the gold schema. They are rebuilt from current silver (and the SCD2 history)
whenever silver changes, and exported as aggregates to `insights.json` for the website. When a
register doesn't publish the fields an insight needs, its country is left out rather than shown
as zero. `gold.insight_coverage` lists each inclusion and exclusion with the reason.

| Table | Question | Registers |
|---|---|---|
| `company_activity_summary` | How many registered companies are actually operating? | all (signals vary: dormancy UK, strike-off UK/IE, mass addresses all) |
| `registered_office_hotspots` | Which addresses host mass registrations? | all with street addresses |
| `company_demography` | Births and deaths per year and industry | births FR GB IE EE NO, Navarra panel entries. Deaths FR IE, register exits |
| `cohort_survival` | Share of new companies still alive after 1/3/5/10 years | FR IE |
| `fishing_hubs` | Where is fishing over-represented (location quotient, per 10,000 inhabitants)? | FR GB IE NO ES |
| `employment_by_region_industry` | Jobs, not just companies | NO (exact) FR ES (bands) |
| `distress_by_industry` | Liquidation / insolvency / strike-off rates | GB IE EE NO |

## Why these

**A registered office is not where a company operates.** In a random slice of the UK register,
one virtual-office address held 1,100 companies, and four such addresses held 3.6% of all
companies; 11% were dormant and 7.5% were being struck off. Plotted as is, a company map mostly
shows where formation agents sit. `dim_company.is_operating` and the map's *Operating companies
only* toggle take them out. An address counts as a mass-registration address once it has
`mass_address_min_companies` active companies (default 50). Natural persons have no street address
in silver, so they never form one.

**Demography depends on what the register keeps.** The UK, Estonian and Norwegian bulk files list
live companies only, so older years are under-counted (`survivorship_biased = true`). Survival can
only be measured where closed units stay in the file with a closure date, which means France and
Ireland. Only cohorts old enough to be observed over the full horizon are counted.

**Fishing hubs.** Ranked by raw counts, big cities come first. The location quotient (town fishing
share ÷ country fishing share) brings the ports up instead. A town needs at least
`fishing_hub_min_companies` fishing companies (default 3) to be ranked.

**Jobs.** Norway reports exact headcounts. France and Navarra publish bands, so the lower and upper
bounds are both kept, and open top bands count at their lower bound.

## Not yet possible (missing source files)

* Estonian activity codes, which are in a separate RIK EMTAK file.
* French legal form and company names, which are in INSEE StockUniteLegale.
* Norwegian workplaces (`underenheter`), which would place fishing jobs at the actual site.
