#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C

# Lanceur reproductible et reprenable pour ns-3.35 / VTP-DSR v4.6.
# Exécuter depuis la racine de ns-3.35.

PROGRAM="${PROGRAM:-scratch/vtp-dsr-comparison}"
PROFILE="${PROFILE:-full-independent}"
RUNS="${RUNS:-20}"
RUN_START="${RUN_START:-1}"
RUN_END="${RUN_END:-$RUNS}"
OUT="${OUT:-vtp-dsr-raw-v4.6.csv}"
RESET_RESULTS="${RESET_RESULTS:-0}"
SKIP_EXISTING="${SKIP_EXISTING:-1}"
SPEEDS="${SPEEDS:-}"
EXTRA_ARGS="${EXTRA_ARGS:-}"
RECOVERY_PACING="${RECOVERY_PACING:-0.005}"
RNG_SEED="${RNG_SEED:-12345}"
CAMPAIGN_ID="${CAMPAIGN_ID:-$(basename "$OUT" .csv)}"
OUTAGE_BASE_START="${OUTAGE_BASE_START:-50.0}"
OUTAGE_PHASE_MODE="${OUTAGE_PHASE_MODE:-balanced}"
OUTAGE_PHASE_SPAN="${OUTAGE_PHASE_SPAN:-30.0}"
OUTAGE_PHASE_BINS="${OUTAGE_PHASE_BINS:-30}"
OUTAGE_PHASE_FIXED="${OUTAGE_PHASE_FIXED:-0.0}"
OUTAGE_PHASE_TABLE="${OUTAGE_PHASE_TABLE:-}"
GRAPH_PATH_DIAGNOSTIC="${GRAPH_PATH_DIAGNOSTIC:-0}"
GRAPH_POLL="${GRAPH_POLL:-0.10}"

case "$PROFILE" in
  static-on)
    SPEEDS="${SPEEDS:-0}"
    SCENARIO_NAME="static-on"
    PHYSICAL_CHANNEL_OUTAGE=0
    PROFILE_ARGS="--scenario=$SCENARIO_NAME --forceRouteAvailable=1 --cycles=0 --range=1000 --areaX=300 --areaY=300 --physicalChannelOutage=0"
    ;;
  static-channel)
    SPEEDS="${SPEEDS:-0}"
    SCENARIO_NAME="static-channel"
    PHYSICAL_CHANNEL_OUTAGE=0
    PROFILE_ARGS="--scenario=$SCENARIO_NAME --forceRouteAvailable=1 --cycles=5 --range=1000 --areaX=300 --areaY=300 --physicalChannelOutage=0"
    ;;
  mobility-route)
    SPEEDS="${SPEEDS:-2 5 10 15 20}"
    SCENARIO_NAME="mobility-route"
    PHYSICAL_CHANNEL_OUTAGE=0
    PROFILE_ARGS="--scenario=$SCENARIO_NAME --cycles=0 --physicalChannelOutage=0"
    ;;
  full-independent)
    SPEEDS="${SPEEDS:-0 2 5 10 15 20}"
    SCENARIO_NAME="full-independent"
    PHYSICAL_CHANNEL_OUTAGE=0
    PROFILE_ARGS="--scenario=$SCENARIO_NAME --cycles=5 --physicalChannelOutage=0"
    ;;
  full-coupled|full)
    SPEEDS="${SPEEDS:-0 2 5 10 15 20}"
    SCENARIO_NAME="full-coupled"
    PHYSICAL_CHANNEL_OUTAGE=1
    PROFILE_ARGS="--scenario=$SCENARIO_NAME --cycles=5 --physicalChannelOutage=1"
    ;;
  custom)
    SPEEDS="${SPEEDS:-5}"
    SCENARIO_NAME="${SCENARIO_NAME:-custom}"
    PHYSICAL_CHANNEL_OUTAGE="${PHYSICAL_CHANNEL_OUTAGE:-1}"
    PROFILE_ARGS="--scenario=$SCENARIO_NAME --physicalChannelOutage=$PHYSICAL_CHANNEL_OUTAGE"
    ;;
  *)
    echo "PROFILE inconnu: $PROFILE" >&2
    exit 1
    ;;
esac

for value in "$RUNS" "$RUN_START" "$RUN_END" "$OUTAGE_PHASE_BINS"; do
  if ! [[ "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "RUNS, RUN_START, RUN_END et OUTAGE_PHASE_BINS doivent être des entiers positifs." >&2
    exit 1
  fi
done
if (( RUN_START > RUN_END )); then
  echo "RUN_START ($RUN_START) doit être <= RUN_END ($RUN_END)." >&2
  exit 1
fi
if (( RUN_END > RUNS )); then
  echo "RUN_END ($RUN_END) doit être <= RUNS ($RUNS), la taille cible de campagne." >&2
  exit 1
fi
if [[ "$CAMPAIGN_ID" == *","* || "$CAMPAIGN_ID" == *$'\n'* ]]; then
  echo "CAMPAIGN_ID ne doit contenir ni virgule ni retour à la ligne." >&2
  exit 1
fi

if [[ "$RESET_RESULTS" == "1" ]]; then
  rm -f "$OUT"
elif [[ -s "$OUT" ]]; then
  header="$(head -n 1 "$OUT")"
  if [[ "$header" != schema_version,* ]]; then
    echo "Refus d'ajouter la v4.6 à un ancien CSV sans schema_version: $OUT" >&2
    echo "Utiliser un nouveau fichier ou RESET_RESULTS=1." >&2
    exit 1
  fi
fi

if [[ -x ./ns3 ]]; then
  RUNNER="ns3"
elif [[ -x ./waf ]]; then
  RUNNER="waf"
else
  echo "Erreur: lancer depuis la racine de ns-3 (./ns3 ou ./waf absent)." >&2
  exit 1
fi

outage_phase_for_run() {
  local run="$1"
  if [[ -n "$OUTAGE_PHASE_TABLE" ]]; then
    if [[ ! -f "$OUTAGE_PHASE_TABLE" ]]; then
      echo "Table de phase introuvable: $OUTAGE_PHASE_TABLE" >&2
      exit 1
    fi
    local phase
    phase="$(awk -F, -v r="$run" 'NR>1 && $1==r {print $2; exit}' "$OUTAGE_PHASE_TABLE")"
    if [[ -z "$phase" ]]; then
      echo "Aucune phase définie pour le run $run dans $OUTAGE_PHASE_TABLE" >&2
      exit 1
    fi
    printf '%.9f' "$phase"
    return
  fi

  case "$OUTAGE_PHASE_MODE" in
    balanced)
      # Stable lorsque la campagne est prolongée: le nombre de classes ne
      # dépend jamais de RUNS/RUN_END.
      awk -v r="$run" -v bins="$OUTAGE_PHASE_BINS" -v span="$OUTAGE_PHASE_SPAN" \
        'BEGIN { bin=(r-1)%bins; printf "%.9f", span*(bin+0.5)/bins }'
      ;;
    fixed)
      printf '%.9f' "$OUTAGE_PHASE_FIXED"
      ;;
    legacy)
      awk -v r="$run" 'BEGIN { printf "%.9f", 5.0*((r-1)%5) }'
      ;;
    *)
      echo "OUTAGE_PHASE_MODE inconnu: $OUTAGE_PHASE_MODE" >&2
      exit 1
      ;;
  esac
}

norm_float() {
  awk -v x="$1" 'BEGIN { printf "%.9f", x+0.0 }'
}

# Index des lignes déjà présentes, pour une reprise sans doublons.
declare -A EXISTING=()
if [[ "$SKIP_EXISTING" == "1" && -s "$OUT" ]]; then
  while IFS=$'\t' read -r key; do
    [[ -n "$key" ]] && EXISTING["$key"]=1
  done < <(python3 - "$OUT" <<'PY'
import csv, sys
path=sys.argv[1]
def f(x): return f"{float(x):.9f}"
with open(path, newline='', encoding='utf-8-sig') as h:
    for row in csv.DictReader(h):
        try:
            key="|".join((
                row['campaign_id'], str(int(float(row['rng_seed']))),
                row['scenario'], row['policy'], f(row['speed']), str(int(float(row['run']))),
                f(row['recovery_pacing_s']), str(int(float(row['physical_channel_outage']))),
                f(row['outage_phase_s']),
            ))
        except (KeyError, ValueError):
            continue
        print(key)
PY
  )
fi

run_one() {
  local policy="$1" speed="$2" run="$3" phase="$4"
  local speed_n pacing_n key
  speed_n="$(norm_float "$speed")"
  pacing_n="$(norm_float "$RECOVERY_PACING")"
  key="${CAMPAIGN_ID}|${RNG_SEED}|${SCENARIO_NAME}|${policy}|${speed_n}|${run}|${pacing_n}|${PHYSICAL_CHANNEL_OUTAGE}|${phase}"

  if [[ "$SKIP_EXISTING" == "1" && -n "${EXISTING[$key]+x}" ]]; then
    echo "SKIP existant: policy=$policy speed=$speed run=$run phase=$phase"
    return
  fi

  local args="--policy=${policy} --speed=${speed} --run=${run} --seed=${RNG_SEED} --campaignId=${CAMPAIGN_ID} --out=${OUT} --recoveryPacing=${RECOVERY_PACING} --outStart=${OUTAGE_BASE_START} --outagePhase=${phase} --graphPathDiagnostic=${GRAPH_PATH_DIAGNOSTIC} --graphPoll=${GRAPH_POLL} ${PROFILE_ARGS} ${EXTRA_ARGS}"
  echo "[${PROFILE}] [${policy}] vitesse=${speed} m/s run=${run} phase=${phase} s pacing=${RECOVERY_PACING} s"
  if [[ "$RUNNER" == "ns3" ]]; then
    ./ns3 run "${PROGRAM} ${args}"
  else
    ./waf --run "${PROGRAM} ${args}"
  fi
  EXISTING["$key"]=1
}

for speed in $SPEEDS; do
  for run in $(seq "$RUN_START" "$RUN_END"); do
    phase="$(outage_phase_for_run "$run")"
    for policy in TRAD VTP1 VTP2; do
      run_one "$policy" "$speed" "$run" "$phase"
    done
  done
done

echo
echo "Campagne terminée: $OUT"
if [[ -n "$OUTAGE_PHASE_TABLE" ]]; then
  phase_description="table=$OUTAGE_PHASE_TABLE"
else
  phase_description="$OUTAGE_PHASE_MODE/$OUTAGE_PHASE_BINS classes"
fi
echo "Profil=$PROFILE; runs=$RUN_START..$RUN_END; pacing=$RECOVERY_PACING s; phase=$phase_description; graph=$GRAPH_PATH_DIAGNOSTIC"
echo "Analyse stricte: python3 analyze-vtp-results.py --input '$OUT' --output-dir vtp-analysis"
