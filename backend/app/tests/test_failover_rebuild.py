from app.modules.failover_offset_rebuild import Evidence, compute_candidate


def _ev(**kw):
    defaults = dict(
        stream_id=1,
        applied_seqs=[],
        sink_last_seqs=[],
        quarantine_seqs=[],
        ledger_applied_seqs=[],
        scan_counts={},
    )
    defaults.update(kw)
    return Evidence(**defaults)


def test_clean_prefix_adoptable():
    ev = _ev(applied_seqs=[1, 2, 3, 4, 5], sink_last_seqs=[5, 4], ledger_applied_seqs=[1, 2, 3, 4, 5])
    r = compute_candidate(ev)
    assert r.adoptable is True
    assert r.candidate == 5
    assert r.gate == "ok"


def test_g1_no_evidence():
    r = compute_candidate(_ev())
    assert r.adoptable is False
    assert r.gate == "G1"
    assert r.candidate == 0


def test_g2_sink_ahead_of_ledger():
    # Sink 出现账本里没有的 seq 9
    ev = _ev(
        applied_seqs=[1, 2, 3],
        sink_last_seqs=[3, 9],
        ledger_applied_seqs=[1, 2, 3],
    )
    r = compute_candidate(ev)
    assert r.adoptable is False
    assert r.gate == "rejected"
    assert any("G2" in x for x in r.reasons)


def test_g3_unexplained_gap_behind_sink():
    # applied 缺 seq 3，但 Sink 已到 4（3 不在隔离区，缺口未解释）
    ev = _ev(
        applied_seqs=[1, 2, 4],
        sink_last_seqs=[4],
        ledger_applied_seqs=[1, 2, 4],
    )
    r = compute_candidate(ev)
    assert r.candidate == 2
    assert r.adoptable is False
    assert any("G3" in x for x in r.reasons)


def test_gap_explained_by_quarantine_still_adoptable_at_prefix():
    # seq 3 被隔离，Sink 只到前缀 2：候选 2 可安全采纳
    ev = _ev(
        applied_seqs=[1, 2],
        sink_last_seqs=[2],
        quarantine_seqs=[3],
        ledger_applied_seqs=[1, 2],
    )
    r = compute_candidate(ev)
    assert r.adoptable is True
    assert r.candidate == 2


def test_g4_quarantine_overlaps_applied():
    ev = _ev(
        applied_seqs=[1, 2],
        sink_last_seqs=[2],
        quarantine_seqs=[2],
        ledger_applied_seqs=[1, 2],
    )
    r = compute_candidate(ev)
    assert r.adoptable is False
    assert any("G4" in x for x in r.reasons)


def test_g5_ledger_sources_disagree():
    ev = _ev(
        applied_seqs=[1, 2, 3],
        sink_last_seqs=[3],
        ledger_applied_seqs=[1, 2],  # change_ledger 少了 seq 3
    )
    r = compute_candidate(ev)
    assert r.adoptable is False
    assert any("G5" in x for x in r.reasons)


def test_sink_smaller_than_ledger_is_ok():
    # 实体被删除后 Sink 不留痕，Sink 偏小是正常的
    ev = _ev(
        applied_seqs=[1, 2, 3, 4],
        sink_last_seqs=[2],
        ledger_applied_seqs=[1, 2, 3, 4],
    )
    r = compute_candidate(ev)
    assert r.adoptable is True
    assert r.candidate == 4


def test_ledger_only_without_sink_adoptable():
    # G1：Sink 为空但账本有连续应用事实，仍可定位
    ev = _ev(applied_seqs=[1, 2], ledger_applied_seqs=[1, 2])
    r = compute_candidate(ev)
    assert r.adoptable is True
    assert r.candidate == 2
