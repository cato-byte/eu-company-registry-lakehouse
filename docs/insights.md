# Insights: what the registers say beyond "how many companies, where"

All tables live in the gold schema, are rebuilt on every run from current silver (and the SCD2
history), and are exported in aggregate form to `insights.json` for the website. A country is
**excluded** from an insight when its register does not publish the needed fields, so an unknown
value is never shown as zero. `gold.insight_coverage` lists every inclusion and exclusion with the reason.

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

* **Registered office ≠ operating location.** In a random slice of the UK register, one virtual-office
  address held 1,100 companies, and four such addresses held 3.6% of all companies. 11% were dormant
  and 7.5% were being struck off. Without this correction a company map mostly shows where formation
  agents are. `dim_company.is_operating` and the map's *Operating companies only* toggle remove them.
  An address counts as a mass-registration address from `mass_address_min_companies` (default 50)
  active companies. Natural persons have no street address, so they can never form one.
* **Demography needs to know what the register keeps.** The UK, Estonian and Norwegian bulk files
  list live companies only, so older years are under-counted (`survivorship_biased = true`), and
  survival can only be measured where closed units are kept with their closure date (France, Ireland).
  Survival counts only cohorts old enough to be observed for the full horizon.
* **Fishing hubs.** Raw counts rank big cities first. The location quotient (town fishing share ÷ country
  fishing share) surfaces the ports. Towns need `fishing_hub_min_companies` (default 3) fishing companies.
* **Jobs.** Exact for Norway. France and Navarra publish bands, so low/high bounds are kept and open
  top bands count at their lower bound.

## Not yet possible (missing source files)

* Estonia activity codes (separate RIK EMTAK file).
* French legal form and company names (INSEE StockUniteLegale).
* Norwegian workplaces (`underenheter`), which would place fishing jobs at the actual site.
