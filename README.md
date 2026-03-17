# OPS Brain

Ops Brain is internal operations intelligence system that gathers operational data from multiple sources into a structured Postgres wareshouse to support decision making.

The system ingests data from platforms such as Shopify, Loop Subscriptions, and Google Sheets planning models, and transforms them into a canonical operational data model used for reporting, forecasting, and inventory planning.

Ops brain acts as the single operational truth laer for revenue, subscriptions, inventory and demand forecasting.

## System Architecture
``` text
External Systems
      ↓
Ingestion Layer (Node.js scripts)
      ↓
Postgres Data Warehouse
      ↓
Operational Views / Reporting
```

## Project Structure
``` text
ops-brain
│
├── .github/workflows/      # scheduled ingestion jobs
├── database/
│   ├── schema/             # database tables
│   └── migrations/         # migration files
│
├── scripts/
│   ├── lib/                # shared utilities
│   ├── shopify/            # Shopify ingestion scripts
│   ├── loop/               # Loop subscription 
│   └── sheets/             # Google Sheets ingestion 
│
└── README.md
```
Ops brain currently intgerates with:
- Shopify
  -  orders
  -  order line items
  -  products
  -  inventory
- Loop subscriptions
  - Loop Orders
  - Loop subscriptions
- Google Sheets
  -  demand forecasts
  - inbound shipment planning
  - financial planning models


## Running the Project

Install dependencies:

```bash
npm install

node scripts/shopify/syncOrders.js
```

### Example:

``` bash
node scripts/loop/syncSubscriptions.js
node scripts/sheets/inboundShipments.js
```


### Automation

Scheduled ingestion jobs are managed through GitHub Actions under:


.github/workflows/

These workflows are used to keep warehouse data updated automatically on a recurring schedule.
