# markov/runtime_prior.py
import json
import pandas as pd

def _bucket_from_value(val: float, edges: dict) -> int:
    for i in range(1, 10):
        if val <= edges[str(i)]:
            return i - 1
    return 9

class PriorLookup:
    def __init__(self,
                 prior_parquet="artifacts/probability_chain/prior_transition.parquet",
                 deciles_json="artifacts/probability_chain/deciles.json"):
        self.prior = pd.read_parquet(prior_parquet)
        with open(deciles_json, "r") as f:
            self.edges = json.load(f)

    def derive_buckets(self, turnover_value: float, atr14_value: float, gap_value: float):
        liq_b = _bucket_from_value(float(turnover_value), self.edges["turnover_deciles"])
        vol_b = _bucket_from_value(float(atr14_value),    self.edges["atr14_deciles"])
        if gap_value < -0.01:
            gap_b = "down"
        elif gap_value > 0.01:
            gap_b = "up"
        else:
            gap_b = "flat"
        return liq_b, vol_b, gap_b

    def get_prior(self, sector: str, liq_b: int, vol_b: int, gap_b: str):
        q = self.prior
        m = q[(q.sector == sector) & (q.liq_bucket == liq_b) &
              (q.vol_bucket == vol_b) & (q.gap_bucket == gap_b)]
        if len(m) == 0:
            m = q[(q.liq_bucket == liq_b) & (q.vol_bucket == vol_b) & (q.gap_bucket == gap_b)]
        if len(m) == 0:
            r = q[["pi_S0_S1", "pi_S1_S2", "pi_S2_S3"]].mean(numeric_only=True)
            return dict(pi_S0_S1=float(r["pi_S0_S1"]),
                        pi_S1_S2=float(r["pi_S1_S2"]),
                        pi_S2_S3=float(r["pi_S2_S3"]),
                        support=0)
        row = m.iloc[0]
        return dict(pi_S0_S1=float(row["pi_S0_S1"]),
                    pi_S1_S2=float(row["pi_S1_S2"]),
                    pi_S2_S3=float(row["pi_S2_S3"]),
                    support=int(row["support"]))
