# docs/3_segmentation_and_scd2.md

## Overview: Segmentation & SCD2 (Gold Layer)

After the data is cleaned in Silver, we move to the Gold Layer for advanced analysis and final form. Our main goal here is to classify or “segment” customers into meaningful groups and keep track of metrics over time.

### Segmentation

We apply an algorithm (e.g., K-Means) to group customers based on behaviors such as:
- How frequently they purchase (churn indicators).
- Their typical spending (CLV).
- Demographics like age, gender, or income.

### Historical Tracking (SCD2)

**Slowly Changing Dimension Type 2 (SCD2)** allows us to:
- Maintain a historical timeline of key metrics (like average CLV by segment).
- Mark older versions as inactive and insert a new "current" version when something changes significantly.

### Example

- If a customer’s buying habits shift from low-value purchases to large electronics spending, their segment might change.
- We close out the old segment record in the table and insert a new “active” record for that customer.

### Business Value

- **Targeted Campaigns**: Each segment might receive different promotions.
- **Historical Insights**: We can see how a segment’s churn rate or average order value has evolved over months or years.
