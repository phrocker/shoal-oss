| Metric | router v2 (dev) | baseline (v2 grammars) (dev) | frozen v1 (dev) | baseline (v1 grammars) (dev) |
|---|---|---|---|---|
| Target precision (non-abstained) | 60 of 61 (98.4%) | 36 of 36 (100.0%) | 8 of 8 (100.0%) | 29 of 29 (100.0%) |
| End-to-end accuracy | 115 of 143 (80.4%) | 91 of 143 (63.6%) | 63 of 143 (44.1%) | 84 of 143 (58.7%) |
| Coverage (answerable) | 61 of 88 (69.3%) | 36 of 88 (40.9%) | 8 of 88 (9.1%) | 29 of 88 (33.0%) |
| Abstention rate | 82 of 143 (57.3%) | 107 of 143 (74.8%) | 135 of 143 (94.4%) | 114 of 143 (79.7%) |
| Correct abstention (unanswerable) | 55 of 55 (100.0%) | 55 of 55 (100.0%) | 55 of 55 (100.0%) | 55 of 55 (100.0%) |
| Slot exact-match (target-correct) | 60 of 60 (100.0%) | 36 of 36 (100.0%) | 8 of 8 (100.0%) | 29 of 29 (100.0%) |
| Abstention reason agreement (secondary) | 49 of 55 (89.1%) | 41 of 55 (74.5%) | 49 of 55 (89.1%) | 44 of 55 (80.0%) |

Discordant pairs (end-to-end): router right and baseline wrong 27, baseline right and router wrong 3; both right 88, both wrong 25; exact McNemar p = 8.43e-06.

Confusion, router v2 (rows expected, columns proposed):

| expected \ proposed | action | decision | lookup | abstain |
|---|---|---|---|---|
| action | 36 | 0 | 0 | 10 |
| decision | 1 | 13 | 0 | 8 |
| lookup | 0 | 0 | 11 | 9 |
| abstain | 0 | 0 | 0 | 55 |

End-to-end by tag, router v2: ambiguous_executor 4 of 4 (100.0%); ambiguous_mention 4 of 4 (100.0%); hidden_node 6 of 6 (100.0%); hidden_target 11 of 12 (91.7%); injection 4 of 4 (100.0%); missing_slot 6 of 6 (100.0%); nonexistent_node 3 of 3 (100.0%); nonexistent_target 3 of 3 (100.0%); plain 48 of 75 (64.0%); slot_mismatch 2 of 2 (100.0%); unanswerable 24 of 24 (100.0%).

Confusion, baseline (v2 grammars) (rows expected, columns proposed):

| expected \ proposed | action | decision | lookup | abstain |
|---|---|---|---|---|
| action | 18 | 0 | 0 | 28 |
| decision | 0 | 12 | 0 | 10 |
| lookup | 0 | 0 | 6 | 14 |
| abstain | 0 | 0 | 0 | 55 |

End-to-end by tag, baseline (v2 grammars): ambiguous_executor 4 of 4 (100.0%); ambiguous_mention 4 of 4 (100.0%); hidden_node 6 of 6 (100.0%); hidden_target 9 of 12 (75.0%); injection 0 of 4 (0.0%); missing_slot 6 of 6 (100.0%); nonexistent_node 3 of 3 (100.0%); nonexistent_target 3 of 3 (100.0%); plain 30 of 75 (40.0%); slot_mismatch 2 of 2 (100.0%); unanswerable 24 of 24 (100.0%).

Confusion, frozen v1 (rows expected, columns proposed):

| expected \ proposed | action | decision | lookup | abstain |
|---|---|---|---|---|
| action | 7 | 0 | 0 | 39 |
| decision | 0 | 0 | 0 | 22 |
| lookup | 0 | 0 | 1 | 19 |
| abstain | 0 | 0 | 0 | 55 |

End-to-end by tag, frozen v1: ambiguous_executor 4 of 4 (100.0%); ambiguous_mention 4 of 4 (100.0%); hidden_node 5 of 6 (83.3%); hidden_target 6 of 12 (50.0%); injection 1 of 4 (25.0%); missing_slot 6 of 6 (100.0%); nonexistent_node 3 of 3 (100.0%); nonexistent_target 3 of 3 (100.0%); plain 5 of 75 (6.7%); slot_mismatch 2 of 2 (100.0%); unanswerable 24 of 24 (100.0%).

Confusion, baseline (v1 grammars) (rows expected, columns proposed):

| expected \ proposed | action | decision | lookup | abstain |
|---|---|---|---|---|
| action | 17 | 0 | 0 | 29 |
| decision | 0 | 9 | 0 | 13 |
| lookup | 0 | 0 | 3 | 17 |
| abstain | 0 | 0 | 0 | 55 |

End-to-end by tag, baseline (v1 grammars): ambiguous_executor 4 of 4 (100.0%); ambiguous_mention 4 of 4 (100.0%); hidden_node 6 of 6 (100.0%); hidden_target 8 of 12 (66.7%); injection 0 of 4 (0.0%); missing_slot 6 of 6 (100.0%); nonexistent_node 3 of 3 (100.0%); nonexistent_target 3 of 3 (100.0%); plain 24 of 75 (32.0%); slot_mismatch 2 of 2 (100.0%); unanswerable 24 of 24 (100.0%).

Router v2 against frozen v1 (end-to-end): router v2 only 52, frozen v1 only 0; both 63, neither 28; exact McNemar p = 4.44e-16.
Router v2 against frozen v1 (coverage, answerable cases): router v2 only 53, frozen v1 only 0; both 8, neither 27; exact McNemar p = 2.22e-16.
Lexical baseline, v2 grammars against v1 grammars (end-to-end): v2 grammars only 10, v1 grammars only 3; both 81, neither 49; exact McNemar p = 0.0923.

G1 (wrong proposals at most 5% of 61, rounded down): true. G2 (correct abstention at least 105 of 112): false.
Primary outcome: negative. H2: supported. S1 (beats frozen v1): true. S2 (coverage at least 96): false. S3 (slot exact-match at least 90%): true.
