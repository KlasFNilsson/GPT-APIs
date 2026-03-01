def _has_content(detail) -> bool:
    # api_client kan returnera antingen wrapper-dict eller redan "data"
    if detail is None:
        return False
    if isinstance(detail, list):
        return len(detail) > 0
    if isinstance(detail, dict):
        data = detail.get("data")
        if isinstance(data, list):
            return len(data) > 0
        if isinstance(data, dict):
            # vanliga listor som indikerar verkligt innehåll
            for k in ("actionInfoList", "actions", "actionList", "trainingActionList", "finishedReps", "finishedRepList"):
                v = data.get(k)
                if isinstance(v, list) and len(v) > 0:
                    return True
            return len(data) > 0
        return len(detail) > 0
    return True


def fetch_training_detail(c: SpeedianceClient, training_id: str, training_type: str | None):
    # Om vi vet typen – prova den först, men fall tillbaka om tomt.
    if training_type in ("course", "ctt"):
        d1 = c.get_training_detail(training_id, training_type)
        if _has_content(d1):
            return d1
        other = "ctt" if training_type == "course" else "course"
        d2 = c.get_training_detail(training_id, other)
        return d2

    # Om typ saknas: prova båda och välj den med innehåll.
    d_course = c.get_training_detail(training_id, "course")
    if _has_content(d_course):
        return d_course

    d_ctt = c.get_training_detail(training_id, "ctt")
    return d_ctt
