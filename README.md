# Simulation VTP1–VTP2 (ns-3.35, DSR)

Reproducibility package of the article *Persistent Cross-Layer Timeouts in DSR-Based
Mobile Ad Hoc Networks: Retransmission-Load Decomposition and Paced Resumption*
(KSII Transactions on Internet and Information Systems).

**Current release: v5.0.5.** The archive `TIIS_VTP_DSR_public_v5.zip` contains the
scenario and DSR sources, the campaign and analysis scripts, the raw CSV data of every
reported figure and table, the timer-schedule model and, since v5.0.5, the no-gate
static run that gives the base delay d0 of Table 4 (`calibration_d0/`).

SHA-256 of the archive (also in `TIIS_VTP_DSR_public_v5.sha256`):
`39c16d569d000698e3ae40b741c88453adf8035e902cd494ec91e00c8a42d7a7`

To check every figure and table, extract the archive and run `python3 reproduce.py`
(Python 3.10+, NumPy, SciPy, pandas, Matplotlib). The archive's own README maps each
figure and table to its data file and script.

The other files at the root of this repository belong to earlier versions and are not
used by the article.
