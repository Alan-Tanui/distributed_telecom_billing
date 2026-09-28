# Distributed Telecom Billing and Rating System

Weeks 3 & 4 — Apache HTTP Server frontend + Power BI dashboard.

See `logs/` for engineering logs and `apache/README.md` for setup.

## Quick start
    pip install -r requirements.txt
    python launcher.py               # in terminal 1
    python experiments/week3_architecture.py   # in terminal 2
    python experiments/week4_election.py
    python experiments/week4_failure.py

## Large-scale workload (e.g. 100,000 requests)
    python experiments/large_scale_workload.py --n 100000 --concurrency 50 --tag baseline

See `powerbi/README.md` for how the --tag flag maps onto Power BI filtering,
and for tips on displaying 100k+ rows cleanly.
