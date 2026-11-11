# Numbers and cost

[← back to the README](../README.md)

These runs are from the first release (October 2026). [CHANGELOG.md](../CHANGELOG.md) says what has changed since and what each change has been run on.

## Numbers

- **First load, dev and prod** (each its own triggered run, about 15 minutes):
  - orders 59,427 · customers 59,427 · order_items 67,114 · order_payments 62,386 · order_reviews 59,373 · products 32,951 · sellers 3,095 · category translation 71;
  - **343,844 rows**, every count equal to the source, 0 duplicate keys, 0 quarantined.
- **Incremental proof** (during development, in the single environment that came before dev and prod):
  - replaying February 2018 added 6,728 new orders on top of existing silver rows;
  - changing one order at the source updated exactly that row in silver;
  - a re-run changed nothing.

  In dev, after the move to `TIMESTAMP_NTZ`, one more changed order again updated exactly one row.
- **Gold:** 13 of 13 checks passed in dev and prod, with every number equal between the two (prod below):

  ![The 13 reconciliation checks in prod](images/gold-reconciliation-checks.png)
- **Time zones:** the source stores local time with no zone. Stored as `TIMESTAMP`, an order delivered at 2018-02-20 23:45 showed as 2018-02-21 05:15 in an India session and landed on the wrong day. Stored as `TIMESTAMP_NTZ` it stays 2018-02-20 23:45 in any session:

  ![TIMESTAMP vs TIMESTAMP_NTZ in an Asia/Kolkata session](images/silver-ntz-timezone-proof.png)
- **A data-quality finding the checks surfaced:** payments (9,416,576.59 BRL) exceed order value (9,294,066.49 BRL) by 122,510.10 BRL. Measured on an earlier copy of the same data, nearly all of the gap is 622 orders that were paid but have no item lines in the source (canceled or unavailable); a small part is card-instalment interest. So the report's revenue is order value — item price plus freight, for orders not canceled or unavailable — not payments.

## Cost

This ran on an Azure for Students subscription.

- The first full load (Data Factory plus one single-node job cluster) cost **at most about ₹30 (well under US$1)** on the meter; a run with gold adds a few minutes of the same cluster.
- Between runs the platform costs almost nothing: the job cluster exists only during a run, the control databases are serverless on the free offer, and there is no SQL warehouse.
- The one expensive lesson came early: developing notebooks on Serverless interactive compute cost about ₹3,000 (about US$35) in three days. Development moved to a small cluster with auto-stop, and every compute choice since has been made with its price first.
