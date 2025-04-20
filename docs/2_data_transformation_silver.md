# docs/2_data_transformation_silver.md

## Overview: Data Preparation (Silver Layer)

The Silver Layer is where we transform and clean the records we ingested. We start making the data more consistent and easier to work with, without losing any critical information.

### Key Operations

1. **Data Cleaning**  
   - Deduplicate records (e.g., if the same purchase accidentally arrives twice).
   - Handle missing values (like imputing age or setting default amounts if fields are null).

2. **Standardization**  
   - Convert date formats into a consistent style.
   - Map categorical fields (e.g., gender "M"/"F" to numerical or standardized text).

3. **Quality Checks**  
   - Ensure numeric fields (purchase_amount, income) fall within reasonable ranges.

### Example

- If the Raw Layer has multiple formats for “payment_date,” we unify them into a single `YYYY-MM-DD HH:MM:SS`.
- If a record has `age = None`, we might set it to a median or fallback age, so we can reliably analyze it in the next phase.

### Why It Matters

- **Improved Consistency**: Downstream consumers can trust the data format.
- **Reduced Errors**: By handling duplicates or strange outliers here, we reduce risk in segmentation and analytics.
