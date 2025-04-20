## Overview: Data Ingestion (Raw Layer)

The Raw Layer represents the very first stage where our incoming data lands. We capture every record of customer activity, whether it’s from Kafka events or other real-time feeds. Think of it as a secure vault where we store everything in its “original” shape, just as it arrives.

### Purpose

- **Traceability**: We want to be able to go back to the exact data as it was captured.
- **Minimal Processing**: No heavy cleaning or transformations here. We preserve the raw record for auditing.

### Example

- Suppose a user makes a purchase at 5 PM via the mobile app. We store the transaction details immediately—customer ID, product category, payment method, etc.—exactly as provided.
- If we discover anomalies later, this raw data is our source of truth.

### Value

- **Risk reduction**: If anything goes wrong downstream, we still have an untouched dataset to fall back on.
- **Simplicity**: This layer is straightforward. Ingest, store, and move on to more detailed processing in the next stage.
