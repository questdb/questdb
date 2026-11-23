#!/usr/bin/env bash
# t15 — the store-and-forward replay is graded by what the client recovered.
#
# NO VM: runs the real guest/verify.sh in compare mode, with stand-ins for the two oracle passes,
# the replay server and the replay client. Seconds.
#
# In compare mode verify.sh measures the table twice, before the client replays (what the server
# alone kept) and after, and the client reports the highest frame it recovered from its buffer.
# The bar is that every recovered row is in the table after the replay. Rows sent after the
# buffer's last sync may be lost (sf_durability=periodic), and that loss is reported, not failed.
# A replay client that reports nothing, or a progress file without the numbers, must never pass.
# What the replay checks find never replaces a failure pass B (the verifier run after the replay)
# reported itself, so a lost acked txn is not filed as a harness fault.
#
# The sweep counts a boundary as proof of the mechanism only when it prints
# "DETAIL SF_REPLAY_PROVEN", and fails a sweep with none, so each case also checks that line: a
# boundary where nothing needed replaying must not count, or a client that stopped syncing would
# leave the sweep green.
#
# Every boundary runs the command harness_verify_cmd builds for the arm, for both arms that replay.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/arms.sh
source "$HERE/../lib/arms.sh"
# shellcheck source=../lib/verdict.sh
source "$HERE/../lib/verdict.sh"
fails=0
oks=0

ok()    { printf '  ok   %s\n' "$1"; oks=$((oks + 1)); }
bad()   { printf '  FAIL %s\n' "$1"; fails=$((fails + 1)); }
check() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (expected '$3', got '$2')"; fi; }

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

echo "t15 — store-and-forward replay grading"

# ---- the guest, relocated -------------------------------------------------------------------
# As in t13: the guest paths become paths under $TMP, and the substitution is checked.
mkdir -p "$TMP/opt/guest" "$TMP/mnt" "$TMP/bin"
sed -e "s#/opt/vmcrash#$TMP/opt#g" -e "s#/mnt/qdb#$TMP/mnt#g" \
    "$HERE/../guest/verify.sh" > "$TMP/opt/guest/verify.sh"
check "the relocated verify.sh holds no guest path" \
    "$(grep -cE '/opt/vmcrash|/mnt/qdb' "$TMP/opt/guest/verify.sh")" "0"
echo "not a jar" > "$TMP/opt/benchmarks.jar"
# The product arm starts and stops the shipped server around the replay; here that is a no-op.
cat > "$TMP/opt/guest/product-dist.sh" <<'EOF'
product_dist_unpack()   { return 0; }
product_dist_version()  { echo test; }
product_server_start()  { return 0; }
product_server_assert() { return 0; }
product_server_stop()   { return 0; }
EOF

# ---- stand-ins --------------------------------------------------------------------------------
# `java` plays each JVM verify.sh starts, from the numbers of the current case in case.env:
#   CrashVerifier pass A  -> distinctIds=$SFA, what the server alone kept
#   CrashVerifier pass B  -> distinctIds=$SFB, and pass B's own verdict line $VERDICT_B, or no
#                            verdict line at all when "none"; $ERR_B, if set, on stderr
#   QwpCrashIngestClient  -> the replay client: reports frame $RFSN, or dies first when "none",
#                            also deletes its log when "nolog", or dies in the middle of a
#                            line when "partial"
#   ServerMain            -> the replay server: nothing to do
cat > "$TMP/bin/java" <<EOF
#!/usr/bin/env bash
source "$TMP/case.env"
res=""
for a in "\$@"; do case "\$a" in -Dresult.file=*) res="\${a#*=}" ;; esac; done
case "\$*" in
    *org.questdb.CrashVerifier*)
        case "\$res" in
            *armA*) printf 'distinctIds=%s\nF=172\nC=170\n' "\$SFA" > "\$res" ;;
            *)      printf 'distinctIds=%s\nF=172\nC=170\n' "\$SFB" > "\$res"
                    [ "\$VERDICT_B" = none ] || echo "\$VERDICT_B"
                    [ -z "\$ERR_B" ] || echo "\$ERR_B" >&2
                    case "\$VERDICT_B" in RPO_OK*) ;; *) exit 1 ;; esac ;;
        esac ;;
    *org.questdb.QwpCrashIngestClient*)
        if [ "\$RFSN" = partial ]; then
            printf 'Exception in thread "main" java.io.IOException: exec fai'
            exit 1
        fi
        if [ "\$RFSN" = nolog ]; then
            rm -f "$TMP/mnt/replay-client.log"
            exit 1
        fi
        if [ "\$RFSN" = none ]; then
            echo 'Exception in thread "main" java.io.IOException: exec failed rc=-1 sql=create table if not exists t'
            exit 1
        fi
        echo "qwp replay-only: sfRecoveredFsn=\$RFSN sfAckedFsn=240" ;;
esac
exit 0
EOF
# pkill MUST be a stand-in: verify.sh runs `pkill -f "[S]erverMain -d"`, which on a shared
# machine kills every other QuestDB server. pgrep would find other runs' verifiers and wait for
# them, and sleep only costs time.
printf '#!/usr/bin/env bash\necho "$*" >> "%s/pkill.calls"\nexit 0\n' "$TMP" > "$TMP/bin/pkill"
printf '#!/usr/bin/env bash\nexit 1\n' > "$TMP/bin/pgrep"
printf '#!/usr/bin/env bash\necho "[[1]]"\nexit 0\n' > "$TMP/bin/curl"
printf '#!/usr/bin/env bash\nexit 0\n' > "$TMP/bin/sleep"
chmod +x "$TMP/bin/"*
export PATH="$TMP/bin:$PATH"
# Bash skips a stand-in it cannot execute, a noexec TMPDIR for one, and verify.sh then runs the
# real command. Recording that as one failed check is not enough: stop before the first case.
for stub in "$TMP"/bin/*; do
    found=$(command -v "${stub##*/}" || true)
    if [ "$found" != "$stub" ]; then
        echo "t15 FAILED: ${stub##*/} resolves to '$found', not the stand-in $stub"
        echo "(a noexec TMPDIR?); stopped before verify.sh could run the real one"
        exit 1
    fi
done
ok "every stand-in is the command verify.sh finds"

# Compare mode is the default for both arms; pinned so an exported value cannot change the test.
export QDB_SF_REPLAY=compare
export QDB_PRODUCT_RECOVERY_PASS=false

RPO_OK_LINE="RPO_OK F=172 >= Wm=168 (every acked txn survived); at-risk txns lost=0 in (Wm=168, C=170] (RPO<=W=50000)"

# grade ARM SFA SFB RFSN SENT BATCH [VERDICT_B [ERR_B]] -> "<token> <proof>", where proof is "proof" when
# the boundary would count as a proven replay and "-" otherwise. SENT or BATCH "none" leaves that
# line out of _qwp_progress. The full output of the last boundary is kept in $TMP/last.out.
grade() {
    local arm="$1" cmd out proof=-
    printf 'SFA=%s\nSFB=%s\nRFSN=%s\nVERDICT_B=%q\nERR_B=%q\n' "$2" "$3" "$4" "${7:-$RPO_OK_LINE}" "${8:-}" \
        > "$TMP/case.env"
    rm -rf "$TMP/mnt/db" "$TMP/mnt/replay-client.log"
    mkdir -p "$TMP/mnt/db"
    {
        printf '%s\nC=170\nWm=168\nrows=%s\n' "$2" "$2"
        [ "$5" = none ] || echo "sent=$5"
        [ "$6" = none ] || echo "batch=$6"
        printf 'ackedFsn=240\nlocalAcks=10\ntrimAdv=10\n'
    } > "$TMP/mnt/db/_qwp_progress"
    cmd=$(harness_verify_cmd "$arm" adaptive 50000 1000 \
        | sed -e "s#/opt/vmcrash#$TMP/opt#g" -e "s#/mnt/qdb#$TMP/mnt#g")
    out=$(eval "$cmd" 2>&1)
    printf '%s\n' "$out" > "$TMP/last.out"
    case "$out" in *"DETAIL SF_REPLAY_PROVEN"*) proof=proof ;; esac
    echo "$(verdict_classify "$(verdict_line "$out")") $proof"
}

# Batches of 1000 rows, so frame N holds rows up to (N + 1) * 1000.
for arm in qwp-sf product; do
    echo "  $arm: the client replayed what it recovered"
    check "$arm, server lost 3000, replay restored them -> RPO_OK, proof" \
        "$(grade "$arm" 244000 247000 246 247000 1000)" "RPO_OK proof"
    check "$arm, replay restored all it recovered, the last 1000 were never synced -> RPO_OK, proof" \
        "$(grade "$arm" 244000 246000 245 247000 1000)" "RPO_OK proof"

    echo "  $arm: nothing to replay, reported and not counted as proof"
    check "$arm, server lost only rows the buffer never synced -> RPO_OK, no proof" \
        "$(grade "$arm" 246000 246000 245 247000 1000)" "RPO_OK -"
    check "$arm, server kept everything sent -> RPO_OK, no proof" \
        "$(grade "$arm" 247000 247000 246 247000 1000)" "RPO_OK -"

    echo "  $arm: recovered rows missing after the replay"
    check "$arm, replay restored 1000 of the 3000 it recovered -> DURABILITY_FAILURE" \
        "$(grade "$arm" 244000 245000 246 247000 1000)" "DURABILITY_FAILURE -"
    check "$arm, replay restored nothing -> DURABILITY_FAILURE" \
        "$(grade "$arm" 244000 244000 246 247000 1000)" "DURABILITY_FAILURE -"
    # Frame 246 holds rows up to 247000, so a replay that stops one frame or one row short of it
    # left recovered rows missing.
    check "$arm, replay left the last recovered frame missing -> DURABILITY_FAILURE" \
        "$(grade "$arm" 244000 246000 246 247000 1000)" "DURABILITY_FAILURE -"
    check "$arm, replay left the last recovered row missing -> DURABILITY_FAILURE" \
        "$(grade "$arm" 244000 246999 246 247000 1000)" "DURABILITY_FAILURE -"
    check "$arm, no sent= and replay restored 1000 of 3000 -> DURABILITY_FAILURE" \
        "$(grade "$arm" 244000 245000 246 none 1000)" "DURABILITY_FAILURE -"
    # A complete replay must not hide pass B's own failure. Only the verdict is checked here.
    r=$(grade "$arm" 244000 247000 246 247000 1000 "DURABILITY_FAILURE F=160 < Wm=168")
    check "$arm, pass B failed although the replay was complete -> DURABILITY_FAILURE" \
        "${r%% *}" DURABILITY_FAILURE

    echo "  $arm: the replay could not be judged"
    check "$arm, replay client died before reporting, server short -> NOT_EVALUATED" \
        "$(grade "$arm" 246000 246000 none 247000 1000)" "NOT_EVALUATED -"
    check "$arm, the verifier's own output is still printed" \
        "$(grep -c '^DETAIL RPO_OK' "$TMP/last.out")" "1"
    check "$arm, the end of the replay client's log is printed" \
        "$(grep -c '^DETAIL SF_REPLAY_CLIENT Exception in thread' "$TMP/last.out")" "1"
    check "$arm, replay client left no log -> NOT_EVALUATED" \
        "$(grade "$arm" 246000 246000 nolog 247000 1000)" "NOT_EVALUATED -"
    check "$arm, replay client killed in the middle of a line, nothing else printed -> NOT_EVALUATED" \
        "$(grade "$arm" 246000 246000 partial 247000 1000 none)" "NOT_EVALUATED -"
    check "$arm, replay client died before reporting, server held everything -> NOT_EVALUATED" \
        "$(grade "$arm" 247000 247000 none 247000 1000)" "NOT_EVALUATED -"
    check "$arm, no batch= in _qwp_progress -> NOT_EVALUATED" \
        "$(grade "$arm" 244000 247000 246 247000 none)" "NOT_EVALUATED -"
    check "$arm, fewer rows after the replay than before -> NOT_EVALUATED" \
        "$(grade "$arm" 247000 246000 246 247000 1000)" "NOT_EVALUATED -"

    echo "  $arm: no sent= in _qwp_progress, replay complete"
    check "$arm, no sent= -> RPO_OK, no proof" \
        "$(grade "$arm" 244000 247000 246 none 1000)" "RPO_OK -"

    echo "  $arm: pass B's own failure is never replaced"
    r=$(grade "$arm" 246000 246000 none 247000 1000 "DURABILITY_FAILURE F=160 < Wm=168")
    check "$arm, pass B failed and the replay client died -> DURABILITY_FAILURE" \
        "${r%% *}" DURABILITY_FAILURE
    check "$arm, what the replay check found is still printed" \
        "$(grep -c '^DETAIL SF_ALSO_FOUND NOT_EVALUATED' "$TMP/last.out")" "1"
    r=$(grade "$arm" 247000 246000 246 247000 1000 "DURABILITY_FAILURE F=160 < Wm=168")
    check "$arm, pass B failed and fewer rows after the replay than before -> DURABILITY_FAILURE" \
        "${r%% *}" DURABILITY_FAILURE
    r=$(grade "$arm" 244000 245000 246 247000 1000 "SILENT_CORRUPTION id=12 has the wrong value")
    check "$arm, pass B found corruption and recovered rows are missing -> SILENT_CORRUPTION" \
        "${r%% *}" SILENT_CORRUPTION

    echo "  $arm: any other pass B verdict gives way to a replay failure"
    check "$arm, pass B gave no verdict and recovered rows are missing -> DURABILITY_FAILURE" \
        "$(grade "$arm" 244000 245000 246 247000 1000 none)" "DURABILITY_FAILURE -"
    # The caller reads the last of the two lines, so keeping them would pass this boundary.
    check "$arm, pass B failed on stdout, RPO_OK on stderr, recovered rows missing -> DURABILITY_FAILURE" \
        "$(grade "$arm" 244000 245000 246 247000 1000 "DURABILITY_FAILURE F=160 < Wm=168" \
            "RPO_OK on stderr")" "DURABILITY_FAILURE -"
    # Kept, this line would be renamed NO_COMMIT further down, which is a pass: nothing committed.
    check "$arm, pass B found no table and recovered rows are missing -> DURABILITY_FAILURE" \
        "$(grade "$arm" 0 0 0 1000 1000 "LOUD_FAILURE: table does not exist [table=t]")" \
        "DURABILITY_FAILURE -"
done

# The classpath arm stops its replay server with pkill; seeing the call proves the stand-in took it.
check "the replay server was stopped through the pkill stand-in" \
    "$(grep -qF 'erverMain -d' "$TMP/pkill.calls" 2>/dev/null && echo yes || echo no)" "yes"

echo
if [ "$fails" -eq 0 ]; then
    echo "t15 PASSED ($oks assertions)"
    exit 0
fi
echo "t15 FAILED: $fails assertion(s) of $((oks + fails))"
exit 1
