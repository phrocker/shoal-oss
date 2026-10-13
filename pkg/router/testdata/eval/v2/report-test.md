| Metric | router v2 (test) | baseline (v2 grammars) (test) | frozen v1 (test) | baseline (v1 grammars) (test) |
|---|---|---|---|---|
| Target precision (non-abstained) | 112 of 112 (100.0%) | 83 of 84 (98.8%) | 17 of 17 (100.0%) | 79 of 79 (100.0%) |
| End-to-end accuracy | 224 of 303 (73.9%) | 195 of 303 (64.4%) | 129 of 303 (42.6%) | 191 of 303 (63.0%) |
| Coverage (answerable) | 112 of 191 (58.6%) | 84 of 191 (44.0%) | 17 of 191 (8.9%) | 79 of 191 (41.4%) |
| Abstention rate | 191 of 303 (63.0%) | 219 of 303 (72.3%) | 286 of 303 (94.4%) | 224 of 303 (73.9%) |
| Correct abstention (unanswerable) | 112 of 112 (100.0%) | 112 of 112 (100.0%) | 112 of 112 (100.0%) | 112 of 112 (100.0%) |
| Slot exact-match (target-correct) | 112 of 112 (100.0%) | 83 of 83 (100.0%) | 17 of 17 (100.0%) | 79 of 79 (100.0%) |
| Abstention reason agreement (secondary) | 89 of 112 (79.5%) | 78 of 112 (69.6%) | 80 of 112 (71.4%) | 79 of 112 (70.5%) |

Discordant pairs (end-to-end): router right and baseline wrong 37, baseline right and router wrong 8; both right 187, both wrong 71; exact McNemar p = 1.537e-05.

Confusion, router v2 (rows expected, columns proposed):

| expected \ proposed | action | decision | lookup | abstain |
|---|---|---|---|---|
| action | 74 | 0 | 0 | 34 |
| decision | 0 | 21 | 0 | 19 |
| lookup | 0 | 0 | 17 | 26 |
| abstain | 0 | 0 | 0 | 112 |

End-to-end by tag, router v2: ambiguous_executor 7 of 7 (100.0%); ambiguous_mention 7 of 7 (100.0%); hidden_node 9 of 10 (90.0%); hidden_target 17 of 22 (77.3%); injection 5 of 6 (83.3%); missing_slot 14 of 14 (100.0%); nonexistent_node 5 of 5 (100.0%); nonexistent_target 6 of 6 (100.0%); plain 97 of 169 (57.4%); slot_mismatch 6 of 6 (100.0%); unanswerable 51 of 51 (100.0%).

Confusion, baseline (v2 grammars) (rows expected, columns proposed):

| expected \ proposed | action | decision | lookup | abstain |
|---|---|---|---|---|
| action | 51 | 0 | 0 | 57 |
| decision | 0 | 19 | 0 | 21 |
| lookup | 0 | 0 | 14 | 29 |
| abstain | 0 | 0 | 0 | 112 |

End-to-end by tag, baseline (v2 grammars): ambiguous_executor 7 of 7 (100.0%); ambiguous_mention 7 of 7 (100.0%); hidden_node 8 of 10 (80.0%); hidden_target 15 of 22 (68.2%); injection 4 of 6 (66.7%); missing_slot 14 of 14 (100.0%); nonexistent_node 5 of 5 (100.0%); nonexistent_target 6 of 6 (100.0%); plain 72 of 169 (42.6%); slot_mismatch 6 of 6 (100.0%); unanswerable 51 of 51 (100.0%).

Confusion, frozen v1 (rows expected, columns proposed):

| expected \ proposed | action | decision | lookup | abstain |
|---|---|---|---|---|
| action | 16 | 0 | 0 | 92 |
| decision | 0 | 0 | 0 | 40 |
| lookup | 0 | 0 | 1 | 42 |
| abstain | 0 | 0 | 0 | 112 |

End-to-end by tag, frozen v1: ambiguous_executor 7 of 7 (100.0%); ambiguous_mention 7 of 7 (100.0%); hidden_node 6 of 10 (60.0%); hidden_target 11 of 22 (50.0%); injection 0 of 6 (0.0%); missing_slot 14 of 14 (100.0%); nonexistent_node 5 of 5 (100.0%); nonexistent_target 6 of 6 (100.0%); plain 16 of 169 (9.5%); slot_mismatch 6 of 6 (100.0%); unanswerable 51 of 51 (100.0%).

Confusion, baseline (v1 grammars) (rows expected, columns proposed):

| expected \ proposed | action | decision | lookup | abstain |
|---|---|---|---|---|
| action | 46 | 0 | 0 | 62 |
| decision | 0 | 20 | 0 | 20 |
| lookup | 0 | 0 | 13 | 30 |
| abstain | 0 | 0 | 0 | 112 |

End-to-end by tag, baseline (v1 grammars): ambiguous_executor 7 of 7 (100.0%); ambiguous_mention 7 of 7 (100.0%); hidden_node 8 of 10 (80.0%); hidden_target 13 of 22 (59.1%); injection 4 of 6 (66.7%); missing_slot 14 of 14 (100.0%); nonexistent_node 5 of 5 (100.0%); nonexistent_target 6 of 6 (100.0%); plain 70 of 169 (41.4%); slot_mismatch 6 of 6 (100.0%); unanswerable 51 of 51 (100.0%).

Router v2 against frozen v1 (end-to-end): router v2 only 95, frozen v1 only 0; both 129, neither 79; exact McNemar p = 5.05e-29.
Router v2 against frozen v1 (coverage, answerable cases): router v2 only 95, frozen v1 only 0; both 17, neither 79; exact McNemar p = 5.05e-29.
Lexical baseline, v2 grammars against v1 grammars (end-to-end): v2 grammars only 19, v1 grammars only 15; both 176, neither 93; exact McNemar p = 0.608.

G1 (wrong proposals at most 5% of 112, rounded down): true. G2 (correct abstention at least 105 of 112): true.
Primary outcome: positive. H2: supported. S1 (beats frozen v1): true. S2 (coverage at least 96): true. S3 (slot exact-match at least 90%): true.
