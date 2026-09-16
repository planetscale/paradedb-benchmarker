# Sourced by the dataset Makefiles, which use Bash with errexit and nounset.
normalize_volume_root() {
    local name=$1 value=$2
    if [[ "$value" != /* || "$value" == *$'\n'* || "$value" == *$'\r'* ]]; then
        echo "$name must be an absolute directory path without newlines" >&2
        return 1
    fi
    command -v realpath >/dev/null || { echo 'Missing dependency: realpath' >&2; return 1; }
    realpath -m -- "$value"
}

volume_root_id() {
    local digest
    digest=$(printf '%s' "$1" | sha256sum)
    printf '%s' "${digest:0:16}"
}

storage_project() {
    if [[ -z "$1" ]]; then printf '%s' "$PROJECT";
    else printf '%s-v%s' "$PROJECT" "$(volume_root_id "$1")"; fi
}

storage_state_dir() {
    if [[ -z "$1" ]]; then printf '%s' "$STATE_DIR";
    else printf '%s/volumes/%s' "$STATE_DIR" "$(volume_root_id "$1")"; fi
}

if [[ -n "$VOLUME_ROOT" ]]; then
    VOLUME_ROOT=$(normalize_volume_root VOLUME_ROOT "$VOLUME_ROOT")
fi
COMPOSE_PROJECT=$(storage_project "$VOLUME_ROOT")
STORAGE_STATE_DIR=$(storage_state_dir "$VOLUME_ROOT")
export VOLUME_ROOT COMPOSE_PROJECT STORAGE_STATE_DIR

check_volume_configuration() {
    local backend volume existing metadata expected
    local -a info
    # Compose can create volume definitions for services not selected in this run.
    # Check this location's definitions before Compose can change any volumes.
    for backend in paradedb pg_textsearch postgres elasticsearch; do
        volume="${COMPOSE_PROJECT}_${backend}_data"
        existing=$(docker volume ls --filter "name=^${volume}$" --format '{{.Name}}')
        [[ -n "$existing" ]] || continue
        metadata=$(docker volume inspect "$volume" --format \
            '{{.Driver}}{{"\n"}}{{len .Options}}{{"\n"}}{{index .Options "type"}}{{"\n"}}{{index .Options "o"}}{{"\n"}}{{index .Options "device"}}')
        mapfile -t info <<< "$metadata"
        if [[ -z "$VOLUME_ROOT" ]]; then
            [[ "${info[0]}" == local && "${info[1]}" == 0 ]] && continue
        else
            expected="$VOLUME_ROOT/$PROJECT/$backend"
            if [[ "${info[0]}" == local && "${info[1]}" == 3 &&
                  "${info[2]}" == none && "${info[3]}" == bind && "${info[4]}" == "$expected" ]]; then
                if [[ ! -d "$expected" ]]; then
                    echo "Existing volume $volume points to a missing directory: $expected" >&2
                    echo 'Restore its storage before continuing; no directory was recreated.' >&2
                    return 1
                fi
                continue
            fi
        fi
        echo "Existing volume $volume uses different storage settings." >&2
        echo 'This storage location has a conflicting Docker volume; its data was left untouched.' >&2
        return 1
    done
}

stop_other_storage_containers() {
    local backend containers container
    local -A stopped=()
    for backend in "$@"; do
        # New containers carry the logical project label; include the original
        # default containers created before this label was introduced as well.
        containers=$(docker ps --filter "label=io.benchmarker.project=$PROJECT" \
            --filter "label=com.docker.compose.service=$backend" --format '{{.Names}}')
        containers+=$'\n'$(docker ps --filter "name=^/${PROJECT}-${backend}$" \
            --filter "label=com.docker.compose.project=$PROJECT" --format '{{.Names}}')
        for container in $containers; do
            [[ "$container" != "$COMPOSE_PROJECT-$backend" && -z "${stopped[$container]:-}" ]] || continue
            echo "Stopping $container before using the selected storage location"
            docker stop --time -1 "$container"
            stopped[$container]=1
        done
    done
}

prepare_volume_directories() {
    local backend
    [[ -n "$VOLUME_ROOT" ]] || return 0
    for backend in paradedb pg_textsearch postgres elasticsearch; do
        mkdir -p -- "$VOLUME_ROOT/$PROJECT/$backend"
    done
}
