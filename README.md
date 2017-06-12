# summer_research

## Fundamental Financial Indicators 
Listed in `crawler/historical_fields_full.json`.

## Other Financial Indicators
### Financial Health (Piotroski, 2000)
***`F_SCORE =	F_ROA + F_ΔROA + F_CFO + F_ACCRUAL + F_ΔMARGIN + F_ΔTURN + F_ΔLEVER + F_ΔLIQUID + EQ_OFFER`***

Definition:

**`F_ROA (F_CFO)`**: one if the firm’s `ROA (CFO)` is positive, zero otherwise.

**`F_ΔROA`**: one if `ΔROA` > 0, zero otherwise.

**`F_ ACCRUAL`**: one if `CFO` > `ROA`, zero otherwise. 

**`F_ΔMARGIN`**: one if `Δmargin` is positive, zero otherwise.

**`F_ΔTURN`**: one if `TURN` is positive, zero otherwise.

**`F_ΔLEVER`**: one (zero) if the firm’s leverage ratio fell (rose) in the year preceding portfolio formation.

**`F_ΔLIQUID`**: one if the firm’s liquidity (current ratio) improved, zero otherwise.

**`EQ_OFFER`**: one if the firm did not issue common equity in the year preceding portfolio formation, zero otherwise.

`ROA`:	Income before extraordinary items, scaled by beginning of the year total assets.

`CFO`:	Cash flow from operations, scaled by beginning of the year total assets.

`ΔROA` : The current year’s ROA less the prior year’s ROA.

`ΔMARGIN`: The firm’s current gross margin ratio (gross margin scaled by total sales) less the prior year’s gross margin ratio.

`ΔTURN`: The firm’s current year asset turnover ratio (total sales scaled by beginning of the year total assets) less the prior year’s asset turnover ratio.

`ΔLEVER`: Historical change in the ratio of total long-term debt to average total assets.

Reference: https://www.chicagobooth.edu/~/media/FE874EE65F624AAEBD0166B1974FD74D.pdf

### Accryal Based on Cash Flow (Bradshaw et al., 2002)
***`AC = Income Before Extraordinary Items − Net Cash Flows from Operating Activities`***
