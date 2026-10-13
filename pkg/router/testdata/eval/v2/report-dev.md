| Metric | router v2 (dev) | baseline (v2 grammars) (dev) | frozen v1 (dev) | baseline (v1 grammars) (dev) |
|---|---|---|---|---|
| Target precision (non-abstained) | 55 of 56 (98.2%) | 31 of 31 (100.0%) | 5 of 5 (100.0%) | 24 of 24 (100.0%) |
| End-to-end accuracy | 103 of 128 (80.5%) | 79 of 128 (61.7%) | 53 of 128 (41.4%) | 72 of 128 (56.2%) |
| Coverage (answerable) | 56 of 80 (70.0%) | 31 of 80 (38.8%) | 5 of 80 (6.2%) | 24 of 80 (30.0%) |
| Abstention rate | 72 of 128 (56.2%) | 97 of 128 (75.8%) | 123 of 128 (96.1%) | 104 of 128 (81.2%) |
| Correct abstention (unanswerable) | 48 of 48 (100.0%) | 48 of 48 (100.0%) | 48 of 48 (100.0%) | 48 of 48 (100.0%) |
| Slot exact-match (target-correct) | 55 of 55 (100.0%) | 31 of 31 (100.0%) | 5 of 5 (100.0%) | 24 of 24 (100.0%) |
| Abstention reason agreement (secondary) | 43 of 48 (89.6%) | 34 of 48 (70.8%) | 42 of 48 (87.5%) | 37 of 48 (77.1%) |

Discordant pairs (end-to-end): router right and baseline wrong 27, baseline right and router wrong 3; both right 76, both wrong 22; exact McNemar p = 8.43e-06.

Confusion, router v2 (rows expected, columns proposed):

| expected \ proposed | action | decision | lookup | abstain |
|---|---|---|---|---|
| action | 33 | 0 | 0 | 9 |
| decision | 1 | 12 | 0 | 7 |
| lookup | 0 | 0 | 10 | 8 |
| abstain | 0 | 0 | 0 | 48 |

End-to-end by tag, router v2: ambiguous_executor 4 of 4 (100.0%); ambiguous_mention 1 of 1 (100.0%); hidden_node 2 of 2 (100.0%); hidden_target 11 of 12 (91.7%); injection 4 of 4 (100.0%); missing_slot 6 of 6 (100.0%); nonexistent_node 3 of 3 (100.0%); nonexistent_target 3 of 3 (100.0%); plain 45 of 69 (65.2%); unanswerable 24 of 24 (100.0%).

Confusion, baseline (v2 grammars) (rows expected, columns proposed):

| expected \ proposed | action | decision | lookup | abstain |
|---|---|---|---|---|
| action | 15 | 0 | 0 | 27 |
| decision | 0 | 11 | 0 | 9 |
| lookup | 0 | 0 | 5 | 13 |
| abstain | 0 | 0 | 0 | 48 |

End-to-end by tag, baseline (v2 grammars): ambiguous_executor 4 of 4 (100.0%); ambiguous_mention 1 of 1 (100.0%); hidden_node 2 of 2 (100.0%); hidden_target 9 of 12 (75.0%); injection 0 of 4 (0.0%); missing_slot 6 of 6 (100.0%); nonexistent_node 3 of 3 (100.0%); nonexistent_target 3 of 3 (100.0%); plain 27 of 69 (39.1%); unanswerable 24 of 24 (100.0%).

Confusion, frozen v1 (rows expected, columns proposed):

| expected \ proposed | action | decision | lookup | abstain |
|---|---|---|---|---|
| action | 5 | 0 | 0 | 37 |
| decision | 0 | 0 | 0 | 20 |
| lookup | 0 | 0 | 0 | 18 |
| abstain | 0 | 0 | 0 | 48 |

End-to-end by tag, frozen v1: ambiguous_executor 4 of 4 (100.0%); ambiguous_mention 1 of 1 (100.0%); hidden_node 1 of 2 (50.0%); hidden_target 6 of 12 (50.0%); injection 1 of 4 (25.0%); missing_slot 6 of 6 (100.0%); nonexistent_node 3 of 3 (100.0%); nonexistent_target 3 of 3 (100.0%); plain 4 of 69 (5.8%); unanswerable 24 of 24 (100.0%).

Confusion, baseline (v1 grammars) (rows expected, columns proposed):

| expected \ proposed | action | decision | lookup | abstain |
|---|---|---|---|---|
| action | 14 | 0 | 0 | 28 |
| decision | 0 | 8 | 0 | 12 |
| lookup | 0 | 0 | 2 | 16 |
| abstain | 0 | 0 | 0 | 48 |

End-to-end by tag, baseline (v1 grammars): ambiguous_executor 4 of 4 (100.0%); ambiguous_mention 1 of 1 (100.0%); hidden_node 2 of 2 (100.0%); hidden_target 8 of 12 (66.7%); injection 0 of 4 (0.0%); missing_slot 6 of 6 (100.0%); nonexistent_node 3 of 3 (100.0%); nonexistent_target 3 of 3 (100.0%); plain 21 of 69 (30.4%); unanswerable 24 of 24 (100.0%).

Router v2 against frozen v1 (end-to-end): router v2 only 50, frozen v1 only 0; both 53, neither 25; exact McNemar p = 1.78e-15.
Router v2 against frozen v1 (coverage, answerable cases): router v2 only 51, frozen v1 only 0; both 5, neither 24; exact McNemar p = 8.88e-16.
Lexical baseline, v2 grammars against v1 grammars (end-to-end): v2 grammars only 10, v1 grammars only 3; both 69, neither 46; exact McNemar p = 0.0923.

G1 (wrong proposals at most 5% of 56, rounded down): true. G2 (correct abstention at least 105 of 112): false.
Primary outcome: negative. H2: supported. S1 (beats frozen v1): true. S2 (coverage at least 96): false. S3 (slot exact-match at least 90%): true.
