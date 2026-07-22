-- Synthetic MeshDB console data
-- seed=42 accounts=100 events_per_account=2 updates=40
-- Generated locally from fixed word lists and Python's pseudo-random generator; no AI is used.
-- Run the schema section of workbench-test.sql first.
-- Paste each statement into Change data using the Data key printed above it.

-- Data key: generated:000001
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000001', 'ada.jones.1.42@example.test', 128393, 1, X'bc1aadbde48b16976c080717');

-- Data key: generated:000001
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317213, 'generated:000001', 'credit', 16659, '{"channel":"mobile","generated":true,"sequence":1}');

-- Data key: generated:000001
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317214, 'generated:000001', 'credit', 6615, '{"channel":"web","generated":true,"sequence":2}');

-- Data key: generated:000002
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000002', 'ravi.ortiz.2.42@example.test', 115574, 1, X'47cfde01c2ce28b26c574727');

-- Data key: generated:000002
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317215, 'generated:000002', 'credit', 11129, '{"channel":"api","generated":true,"sequence":1}');

-- Data key: generated:000002
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317216, 'generated:000002', 'credit', 3269, '{"channel":"console","generated":true,"sequence":2}');

-- Data key: generated:000003
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000003', 'lina.vega.3.42@example.test', 138685, 1, X'ba75891ff9ec60148d4bd4a0');

-- Data key: generated:000003
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317217, 'generated:000003', 'debit', 11950, '{"channel":"mobile","generated":true,"sequence":1}');

-- Data key: generated:000003
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317218, 'generated:000003', 'credit', 2379, '{"channel":"api","generated":true,"sequence":2}');

-- Data key: generated:000004
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000004', 'hiro.khan.4.42@example.test', 41833, 0, X'dd19614774a2d55d295e5a35');

-- Data key: generated:000004
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317219, 'generated:000004', 'debit', 23097, '{"channel":"web","generated":true,"sequence":1}');

-- Data key: generated:000004
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317220, 'generated:000004', 'debit', 20060, '{"channel":"web","generated":true,"sequence":2}');

-- Data key: generated:000005
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000005', 'farah.tan.5.42@example.test', 382272, 1, X'766145fdeca3b08e38af53d7');

-- Data key: generated:000005
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317221, 'generated:000005', 'debit', 1932, '{"channel":"batch","generated":true,"sequence":1}');

-- Data key: generated:000005
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317222, 'generated:000005', 'note', NULL, '{"channel":"console","generated":true,"sequence":2}');

-- Data key: generated:000006
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000006', 'mateo.jones.6.42@example.test', 34701, 1, X'f191e0b75036a77f65e2eaa4');

-- Data key: generated:000006
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317223, 'generated:000006', 'credit', 8779, '{"channel":"batch","generated":true,"sequence":1}');

-- Data key: generated:000006
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317224, 'generated:000006', 'credit', 18494, '{"channel":"mobile","generated":true,"sequence":2}');

-- Data key: generated:000007
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000007', 'iris.usman.7.42@example.test', 224622, 0, X'665c38ffff23827e17c10cdc');

-- Data key: generated:000007
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317225, 'generated:000007', 'credit', 20660, '{"channel":"batch","generated":true,"sequence":1}');

-- Data key: generated:000007
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317226, 'generated:000007', 'debit', 13933, '{"channel":"mobile","generated":true,"sequence":2}');

-- Data key: generated:000008
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000008', 'chen.ng.8.42@example.test', 200078, 1, X'778740f88ddcf102aeb81dae');

-- Data key: generated:000008
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317227, 'generated:000008', 'note', NULL, '{"channel":"console","generated":true,"sequence":1}');

-- Data key: generated:000008
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317228, 'generated:000008', 'debit', 11246, '{"channel":"api","generated":true,"sequence":2}');

-- Data key: generated:000009
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000009', 'jamal.ortiz.9.42@example.test', 82920, 1, X'f4b8e0b843f880c32d81e91b');

-- Data key: generated:000009
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317229, 'generated:000009', 'note', NULL, '{"channel":"console","generated":true,"sequence":1}');

-- Data key: generated:000009
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317230, 'generated:000009', 'note', NULL, '{"channel":"mobile","generated":true,"sequence":2}');

-- Data key: generated:000010
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000010', 'tariq.hassan.10.42@example.test', 80131, 1, X'298af4c7ec87eb0099527d04');

-- Data key: generated:000010
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317231, 'generated:000010', 'credit', 11994, '{"channel":"console","generated":true,"sequence":1}');

-- Data key: generated:000010
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317232, 'generated:000010', 'credit', 7992, '{"channel":"mobile","generated":true,"sequence":2}');

-- Data key: generated:000011
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000011', 'chen.das.11.42@example.test', 383730, 1, X'11fac288c42020a879f28c2a');

-- Data key: generated:000011
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317233, 'generated:000011', 'credit', 19976, '{"channel":"import","generated":true,"sequence":1}');

-- Data key: generated:000011
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317234, 'generated:000011', 'note', NULL, '{"channel":"mobile","generated":true,"sequence":2}');

-- Data key: generated:000012
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000012', 'grace.khan.12.42@example.test', 209186, 0, X'a65f70e684731e3f39105605');

-- Data key: generated:000012
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317235, 'generated:000012', 'debit', 7640, '{"channel":"mobile","generated":true,"sequence":1}');

-- Data key: generated:000012
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317236, 'generated:000012', 'credit', 2426, '{"channel":"web","generated":true,"sequence":2}');

-- Data key: generated:000013
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000013', 'amina.ito.13.42@example.test', 35337, 0, X'dc5412833c47ab7c368a21b9');

-- Data key: generated:000013
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317237, 'generated:000013', 'note', NULL, '{"channel":"mobile","generated":true,"sequence":1}');

-- Data key: generated:000013
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317238, 'generated:000013', 'debit', 8062, '{"channel":"import","generated":true,"sequence":2}');

-- Data key: generated:000014
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000014', 'nora.hassan.14.42@example.test', 49453, 1, X'6e5a6c6977ddba0daca7fba5');

-- Data key: generated:000014
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317239, 'generated:000014', 'credit', 13293, '{"channel":"web","generated":true,"sequence":1}');

-- Data key: generated:000014
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317240, 'generated:000014', 'credit', 3680, '{"channel":"batch","generated":true,"sequence":2}');

-- Data key: generated:000015
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000015', 'grace.hassan.15.42@example.test', 281168, 1, X'6c2e47763fdfec1371cedcdb');

-- Data key: generated:000015
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317241, 'generated:000015', 'debit', 1757, '{"channel":"web","generated":true,"sequence":1}');

-- Data key: generated:000015
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317242, 'generated:000015', 'note', NULL, '{"channel":"api","generated":true,"sequence":2}');

-- Data key: generated:000016
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000016', 'chen.ito.16.42@example.test', 87194, 1, X'7b36dd66e70f2a6100fc6343');

-- Data key: generated:000016
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317243, 'generated:000016', 'note', NULL, '{"channel":"import","generated":true,"sequence":1}');

-- Data key: generated:000016
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317244, 'generated:000016', 'credit', 22925, '{"channel":"web","generated":true,"sequence":2}');

-- Data key: generated:000017
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000017', 'ravi.reed.17.42@example.test', 81158, 1, X'37f70e94bc8a0fbf500e0c95');

-- Data key: generated:000017
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317245, 'generated:000017', 'credit', 17503, '{"channel":"batch","generated":true,"sequence":1}');

-- Data key: generated:000017
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317246, 'generated:000017', 'credit', 16740, '{"channel":"api","generated":true,"sequence":2}');

-- Data key: generated:000018
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000018', 'farah.das.18.42@example.test', 311969, 1, X'dc3c671ef1e3913f94980a9e');

-- Data key: generated:000018
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317247, 'generated:000018', 'credit', 21640, '{"channel":"mobile","generated":true,"sequence":1}');

-- Data key: generated:000018
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317248, 'generated:000018', 'debit', 10466, '{"channel":"console","generated":true,"sequence":2}');

-- Data key: generated:000019
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000019', 'grace.lee.19.42@example.test', 125140, 1, X'21aba54c7550edc0ef120275');

-- Data key: generated:000019
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317249, 'generated:000019', 'debit', 18548, '{"channel":"api","generated":true,"sequence":1}');

-- Data key: generated:000019
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317250, 'generated:000019', 'credit', 7084, '{"channel":"mobile","generated":true,"sequence":2}');

-- Data key: generated:000020
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000020', 'iris.fischer.20.42@example.test', 489296, 1, X'11e13e5e482870d58bb44d9c');

-- Data key: generated:000020
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317251, 'generated:000020', 'note', NULL, '{"channel":"web","generated":true,"sequence":1}');

-- Data key: generated:000020
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317252, 'generated:000020', 'debit', 21984, '{"channel":"mobile","generated":true,"sequence":2}');

-- Data key: generated:000021
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000021', 'jamal.evans.21.42@example.test', 492269, 0, X'431de31bbe8d2745489a35b7');

-- Data key: generated:000021
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317253, 'generated:000021', 'credit', 22627, '{"channel":"web","generated":true,"sequence":1}');

-- Data key: generated:000021
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317254, 'generated:000021', 'note', NULL, '{"channel":"mobile","generated":true,"sequence":2}');

-- Data key: generated:000022
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000022', 'priya.jones.22.42@example.test', 474657, 0, X'0d17a26cd4460b0055c521a3');

-- Data key: generated:000022
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317255, 'generated:000022', 'note', NULL, '{"channel":"batch","generated":true,"sequence":1}');

-- Data key: generated:000022
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317256, 'generated:000022', 'debit', 18177, '{"channel":"web","generated":true,"sequence":2}');

-- Data key: generated:000023
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000023', 'nora.tan.23.42@example.test', 5069, 1, X'f1e2b0e7268b09d55e958d25');

-- Data key: generated:000023
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317257, 'generated:000023', 'credit', 1470, '{"channel":"console","generated":true,"sequence":1}');

-- Data key: generated:000023
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317258, 'generated:000023', 'credit', 1407, '{"channel":"console","generated":true,"sequence":2}');

-- Data key: generated:000024
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000024', 'grace.ito.24.42@example.test', 349665, 1, X'c78fe2df68f99ebf27ecee3c');

-- Data key: generated:000024
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317259, 'generated:000024', 'note', NULL, '{"channel":"batch","generated":true,"sequence":1}');

-- Data key: generated:000024
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317260, 'generated:000024', 'note', NULL, '{"channel":"api","generated":true,"sequence":2}');

-- Data key: generated:000025
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000025', 'farah.lee.25.42@example.test', 410195, 0, X'cdabddbccf3f4428c9b31b61');

-- Data key: generated:000025
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317261, 'generated:000025', 'note', NULL, '{"channel":"import","generated":true,"sequence":1}');

-- Data key: generated:000025
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317262, 'generated:000025', 'credit', 15183, '{"channel":"console","generated":true,"sequence":2}');

-- Data key: generated:000026
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000026', 'jamal.ito.26.42@example.test', 116876, 1, X'31665447dd11f7c54759a482');

-- Data key: generated:000026
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317263, 'generated:000026', 'credit', 17670, '{"channel":"console","generated":true,"sequence":1}');

-- Data key: generated:000026
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317264, 'generated:000026', 'note', NULL, '{"channel":"api","generated":true,"sequence":2}');

-- Data key: generated:000027
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000027', 'iris.garcia.27.42@example.test', 304396, 0, X'43091b986f58bac9506f9bfb');

-- Data key: generated:000027
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317265, 'generated:000027', 'debit', 12722, '{"channel":"mobile","generated":true,"sequence":1}');

-- Data key: generated:000027
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317266, 'generated:000027', 'credit', 1554, '{"channel":"web","generated":true,"sequence":2}');

-- Data key: generated:000028
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000028', 'nora.baker.28.42@example.test', 272587, 0, X'89afb8f0bdbcab325d6e11f2');

-- Data key: generated:000028
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317267, 'generated:000028', 'debit', 10919, '{"channel":"mobile","generated":true,"sequence":1}');

-- Data key: generated:000028
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317268, 'generated:000028', 'credit', 4183, '{"channel":"web","generated":true,"sequence":2}');

-- Data key: generated:000029
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000029', 'jamal.singh.29.42@example.test', 162154, 1, X'5367b24b8d20316baaf061ad');

-- Data key: generated:000029
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317269, 'generated:000029', 'debit', 5802, '{"channel":"mobile","generated":true,"sequence":1}');

-- Data key: generated:000029
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317270, 'generated:000029', 'debit', 13406, '{"channel":"mobile","generated":true,"sequence":2}');

-- Data key: generated:000030
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000030', 'ada.khan.30.42@example.test', 150425, 1, X'c9949ba752777171ac368279');

-- Data key: generated:000030
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317271, 'generated:000030', 'debit', 24215, '{"channel":"batch","generated":true,"sequence":1}');

-- Data key: generated:000030
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317272, 'generated:000030', 'debit', 9399, '{"channel":"mobile","generated":true,"sequence":2}');

-- Data key: generated:000031
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000031', 'tariq.lee.31.42@example.test', 48961, 1, X'c03cac4f39ce3225060b3efb');

-- Data key: generated:000031
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317273, 'generated:000031', 'credit', 2486, '{"channel":"import","generated":true,"sequence":1}');

-- Data key: generated:000031
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317274, 'generated:000031', 'credit', 20736, '{"channel":"mobile","generated":true,"sequence":2}');

-- Data key: generated:000032
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000032', 'grace.ng.32.42@example.test', 259196, 1, X'25a7b001e4c0dcc5e21bc76c');

-- Data key: generated:000032
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317275, 'generated:000032', 'credit', 22903, '{"channel":"mobile","generated":true,"sequence":1}');

-- Data key: generated:000032
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317276, 'generated:000032', 'credit', 18365, '{"channel":"batch","generated":true,"sequence":2}');

-- Data key: generated:000033
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000033', 'diego.patel.33.42@example.test', 69908, 1, X'aa87fc8f9851f3c1e4719cd0');

-- Data key: generated:000033
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317277, 'generated:000033', 'debit', 16640, '{"channel":"import","generated":true,"sequence":1}');

-- Data key: generated:000033
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317278, 'generated:000033', 'note', NULL, '{"channel":"mobile","generated":true,"sequence":2}');

-- Data key: generated:000034
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000034', 'omar.garcia.34.42@example.test', 389889, 0, X'7342c03fd7a346c4c7857ca0');

-- Data key: generated:000034
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317279, 'generated:000034', 'credit', 14513, '{"channel":"api","generated":true,"sequence":1}');

-- Data key: generated:000034
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317280, 'generated:000034', 'debit', 7783, '{"channel":"console","generated":true,"sequence":2}');

-- Data key: generated:000035
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000035', 'kai.lee.35.42@example.test', 468203, 1, X'23263b62b127b436106a6854');

-- Data key: generated:000035
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317281, 'generated:000035', 'debit', 13724, '{"channel":"api","generated":true,"sequence":1}');

-- Data key: generated:000035
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317282, 'generated:000035', 'credit', 13867, '{"channel":"import","generated":true,"sequence":2}');

-- Data key: generated:000036
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000036', 'sofia.baker.36.42@example.test', 449174, 0, X'93617a01f15a4cc063dae4f4');

-- Data key: generated:000036
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317283, 'generated:000036', 'note', NULL, '{"channel":"mobile","generated":true,"sequence":1}');

-- Data key: generated:000036
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317284, 'generated:000036', 'debit', 17995, '{"channel":"mobile","generated":true,"sequence":2}');

-- Data key: generated:000037
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000037', 'hiro.reed.37.42@example.test', 115040, 1, X'7c076356abadcc67b92ad777');

-- Data key: generated:000037
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317285, 'generated:000037', 'note', NULL, '{"channel":"mobile","generated":true,"sequence":1}');

-- Data key: generated:000037
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317286, 'generated:000037', 'debit', 13011, '{"channel":"mobile","generated":true,"sequence":2}');

-- Data key: generated:000038
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000038', 'sofia.baker.38.42@example.test', 44012, 1, X'22dd762e0c42615336745356');

-- Data key: generated:000038
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317287, 'generated:000038', 'debit', 12523, '{"channel":"console","generated":true,"sequence":1}');

-- Data key: generated:000038
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317288, 'generated:000038', 'debit', 13913, '{"channel":"console","generated":true,"sequence":2}');

-- Data key: generated:000039
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000039', 'chen.reed.39.42@example.test', 10162, 1, X'0dfff35939a611c7f5a60ac1');

-- Data key: generated:000039
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317289, 'generated:000039', 'credit', 8202, '{"channel":"batch","generated":true,"sequence":1}');

-- Data key: generated:000039
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317290, 'generated:000039', 'note', NULL, '{"channel":"mobile","generated":true,"sequence":2}');

-- Data key: generated:000040
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000040', 'elena.ito.40.42@example.test', 66176, 1, X'1d90f23777b341c45e2a9b9b');

-- Data key: generated:000040
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317291, 'generated:000040', 'note', NULL, '{"channel":"web","generated":true,"sequence":1}');

-- Data key: generated:000040
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317292, 'generated:000040', 'credit', 5466, '{"channel":"console","generated":true,"sequence":2}');

-- Data key: generated:000041
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000041', 'diego.usman.41.42@example.test', 13462, 0, X'93ade8f56065f1b7321397b0');

-- Data key: generated:000041
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317293, 'generated:000041', 'note', NULL, '{"channel":"batch","generated":true,"sequence":1}');

-- Data key: generated:000041
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317294, 'generated:000041', 'credit', 9982, '{"channel":"web","generated":true,"sequence":2}');

-- Data key: generated:000042
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000042', 'tariq.evans.42.42@example.test', 417452, 0, X'c80a58886da95e1181a55703');

-- Data key: generated:000042
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317295, 'generated:000042', 'note', NULL, '{"channel":"import","generated":true,"sequence":1}');

-- Data key: generated:000042
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317296, 'generated:000042', 'credit', 11968, '{"channel":"web","generated":true,"sequence":2}');

-- Data key: generated:000043
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000043', 'omar.fischer.43.42@example.test', 228320, 1, X'85f7a6459dceeb89c67b776f');

-- Data key: generated:000043
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317297, 'generated:000043', 'note', NULL, '{"channel":"mobile","generated":true,"sequence":1}');

-- Data key: generated:000043
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317298, 'generated:000043', 'credit', 8144, '{"channel":"api","generated":true,"sequence":2}');

-- Data key: generated:000044
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000044', 'iris.patel.44.42@example.test', 127854, 1, X'919cab6156077ed9532e7c36');

-- Data key: generated:000044
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317299, 'generated:000044', 'credit', 8565, '{"channel":"console","generated":true,"sequence":1}');

-- Data key: generated:000044
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317300, 'generated:000044', 'credit', 19634, '{"channel":"web","generated":true,"sequence":2}');

-- Data key: generated:000045
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000045', 'iris.tan.45.42@example.test', 5322, 1, X'30153db8687d8ec23db079a5');

-- Data key: generated:000045
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317301, 'generated:000045', 'debit', 14785, '{"channel":"api","generated":true,"sequence":1}');

-- Data key: generated:000045
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317302, 'generated:000045', 'credit', 7361, '{"channel":"import","generated":true,"sequence":2}');

-- Data key: generated:000046
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000046', 'hiro.khan.46.42@example.test', 348106, 1, X'798d87586cffbe8c545ab374');

-- Data key: generated:000046
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317303, 'generated:000046', 'credit', 8337, '{"channel":"batch","generated":true,"sequence":1}');

-- Data key: generated:000046
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317304, 'generated:000046', 'credit', 6410, '{"channel":"console","generated":true,"sequence":2}');

-- Data key: generated:000047
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000047', 'diego.tan.47.42@example.test', 498533, 1, X'2f3137bd7b46b996fac28698');

-- Data key: generated:000047
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317305, 'generated:000047', 'credit', 3394, '{"channel":"batch","generated":true,"sequence":1}');

-- Data key: generated:000047
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317306, 'generated:000047', 'credit', 11925, '{"channel":"batch","generated":true,"sequence":2}');

-- Data key: generated:000048
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000048', 'jamal.baker.48.42@example.test', 371205, 1, X'460bf90d8d4ab2f120a3dec0');

-- Data key: generated:000048
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317307, 'generated:000048', 'credit', 501, '{"channel":"mobile","generated":true,"sequence":1}');

-- Data key: generated:000048
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317308, 'generated:000048', 'credit', 15786, '{"channel":"import","generated":true,"sequence":2}');

-- Data key: generated:000049
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000049', 'kai.garcia.49.42@example.test', 26936, 1, X'dc7a1dd210667d1293a1af0d');

-- Data key: generated:000049
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317309, 'generated:000049', 'credit', 18543, '{"channel":"console","generated":true,"sequence":1}');

-- Data key: generated:000049
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317310, 'generated:000049', 'credit', 8232, '{"channel":"api","generated":true,"sequence":2}');

-- Data key: generated:000050
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000050', 'ravi.ortiz.50.42@example.test', 317885, 1, X'9e39c6856173e8714cdc96fd');

-- Data key: generated:000050
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317311, 'generated:000050', 'credit', 18732, '{"channel":"mobile","generated":true,"sequence":1}');

-- Data key: generated:000050
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317312, 'generated:000050', 'credit', 24351, '{"channel":"api","generated":true,"sequence":2}');

-- Data key: generated:000051
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000051', 'grace.hassan.51.42@example.test', 138750, 1, X'283d2c8d1328006873b09878');

-- Data key: generated:000051
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317313, 'generated:000051', 'credit', 7685, '{"channel":"console","generated":true,"sequence":1}');

-- Data key: generated:000051
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317314, 'generated:000051', 'debit', 23135, '{"channel":"import","generated":true,"sequence":2}');

-- Data key: generated:000052
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000052', 'chen.ito.52.42@example.test', 484395, 1, X'caa096a9cdef326c1d8b39a5');

-- Data key: generated:000052
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317315, 'generated:000052', 'credit', 8804, '{"channel":"batch","generated":true,"sequence":1}');

-- Data key: generated:000052
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317316, 'generated:000052', 'credit', 5536, '{"channel":"console","generated":true,"sequence":2}');

-- Data key: generated:000053
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000053', 'tariq.usman.53.42@example.test', 482970, 1, X'1f77b04db367f145808a7e70');

-- Data key: generated:000053
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317317, 'generated:000053', 'credit', 1406, '{"channel":"import","generated":true,"sequence":1}');

-- Data key: generated:000053
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317318, 'generated:000053', 'debit', 19882, '{"channel":"console","generated":true,"sequence":2}');

-- Data key: generated:000054
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000054', 'ada.das.54.42@example.test', 120022, 0, X'd6dc9396f305ffc3acd24493');

-- Data key: generated:000054
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317319, 'generated:000054', 'credit', 24881, '{"channel":"batch","generated":true,"sequence":1}');

-- Data key: generated:000054
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317320, 'generated:000054', 'credit', 21448, '{"channel":"import","generated":true,"sequence":2}');

-- Data key: generated:000055
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000055', 'iris.garcia.55.42@example.test', 306881, 1, X'd07df8177859685552ab1adb');

-- Data key: generated:000055
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317321, 'generated:000055', 'credit', 13588, '{"channel":"web","generated":true,"sequence":1}');

-- Data key: generated:000055
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317322, 'generated:000055', 'credit', 21811, '{"channel":"import","generated":true,"sequence":2}');

-- Data key: generated:000056
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000056', 'ravi.costa.56.42@example.test', 238459, 1, X'40521df8c567dd83d3fc00a8');

-- Data key: generated:000056
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317323, 'generated:000056', 'note', NULL, '{"channel":"import","generated":true,"sequence":1}');

-- Data key: generated:000056
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317324, 'generated:000056', 'credit', 6246, '{"channel":"mobile","generated":true,"sequence":2}');

-- Data key: generated:000057
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000057', 'lina.vega.57.42@example.test', 396594, 1, X'71c20d34448c21ed4970e1b2');

-- Data key: generated:000057
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317325, 'generated:000057', 'credit', 1045, '{"channel":"web","generated":true,"sequence":1}');

-- Data key: generated:000057
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317326, 'generated:000057', 'debit', 7941, '{"channel":"web","generated":true,"sequence":2}');

-- Data key: generated:000058
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000058', 'farah.khan.58.42@example.test', 288798, 1, X'681739fed7e91d76f21ea5d5');

-- Data key: generated:000058
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317327, 'generated:000058', 'credit', 23581, '{"channel":"console","generated":true,"sequence":1}');

-- Data key: generated:000058
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317328, 'generated:000058', 'debit', 9059, '{"channel":"import","generated":true,"sequence":2}');

-- Data key: generated:000059
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000059', 'priya.reed.59.42@example.test', 127785, 1, X'256230eb9982bfe122dd1146');

-- Data key: generated:000059
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317329, 'generated:000059', 'debit', 13696, '{"channel":"console","generated":true,"sequence":1}');

-- Data key: generated:000059
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317330, 'generated:000059', 'note', NULL, '{"channel":"mobile","generated":true,"sequence":2}');

-- Data key: generated:000060
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000060', 'iris.baker.60.42@example.test', 148289, 1, X'd69694ffa87ddd2672897b58');

-- Data key: generated:000060
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317331, 'generated:000060', 'credit', 17907, '{"channel":"import","generated":true,"sequence":1}');

-- Data key: generated:000060
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317332, 'generated:000060', 'credit', 10644, '{"channel":"batch","generated":true,"sequence":2}');

-- Data key: generated:000061
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000061', 'hiro.usman.61.42@example.test', 200787, 1, X'c6690b51be79b4e9cf6162fd');

-- Data key: generated:000061
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317333, 'generated:000061', 'debit', 21465, '{"channel":"batch","generated":true,"sequence":1}');

-- Data key: generated:000061
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317334, 'generated:000061', 'credit', 1313, '{"channel":"batch","generated":true,"sequence":2}');

-- Data key: generated:000062
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000062', 'quinn.usman.62.42@example.test', 174060, 0, X'dfd8701986e97403b82468de');

-- Data key: generated:000062
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317335, 'generated:000062', 'debit', 5158, '{"channel":"api","generated":true,"sequence":1}');

-- Data key: generated:000062
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317336, 'generated:000062', 'credit', 8784, '{"channel":"console","generated":true,"sequence":2}');

-- Data key: generated:000063
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000063', 'tariq.ng.63.42@example.test', 340701, 1, X'54daacdb8861f451a0b7e3c2');

-- Data key: generated:000063
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317337, 'generated:000063', 'credit', 17828, '{"channel":"api","generated":true,"sequence":1}');

-- Data key: generated:000063
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317338, 'generated:000063', 'debit', 7792, '{"channel":"web","generated":true,"sequence":2}');

-- Data key: generated:000064
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000064', 'jamal.ito.64.42@example.test', 391520, 1, X'fa19c2a2b4df19712ab14ce7');

-- Data key: generated:000064
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317339, 'generated:000064', 'credit', 10729, '{"channel":"api","generated":true,"sequence":1}');

-- Data key: generated:000064
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317340, 'generated:000064', 'credit', 12382, '{"channel":"import","generated":true,"sequence":2}');

-- Data key: generated:000065
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000065', 'elena.ito.65.42@example.test', 278484, 1, X'aeca2e2b2c149cde619eae3d');

-- Data key: generated:000065
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317341, 'generated:000065', 'credit', 19208, '{"channel":"batch","generated":true,"sequence":1}');

-- Data key: generated:000065
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317342, 'generated:000065', 'credit', 21002, '{"channel":"console","generated":true,"sequence":2}');

-- Data key: generated:000066
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000066', 'omar.jones.66.42@example.test', 349616, 1, X'cd77e649ad8b281271f158fc');

-- Data key: generated:000066
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317343, 'generated:000066', 'debit', 21036, '{"channel":"import","generated":true,"sequence":1}');

-- Data key: generated:000066
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317344, 'generated:000066', 'debit', 15069, '{"channel":"console","generated":true,"sequence":2}');

-- Data key: generated:000067
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000067', 'grace.ng.67.42@example.test', 447393, 1, X'3c61925b934bfeb34b05fad4');

-- Data key: generated:000067
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317345, 'generated:000067', 'debit', 9094, '{"channel":"api","generated":true,"sequence":1}');

-- Data key: generated:000067
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317346, 'generated:000067', 'debit', 22572, '{"channel":"web","generated":true,"sequence":2}');

-- Data key: generated:000068
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000068', 'amina.vega.68.42@example.test', 390684, 1, X'e7e749c6cc3a9bcd5a38a230');

-- Data key: generated:000068
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317347, 'generated:000068', 'debit', 22308, '{"channel":"web","generated":true,"sequence":1}');

-- Data key: generated:000068
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317348, 'generated:000068', 'debit', 22410, '{"channel":"batch","generated":true,"sequence":2}');

-- Data key: generated:000069
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000069', 'diego.costa.69.42@example.test', 161969, 1, X'08945dbb2117e84b53bf6a2c');

-- Data key: generated:000069
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317349, 'generated:000069', 'credit', 17775, '{"channel":"console","generated":true,"sequence":1}');

-- Data key: generated:000069
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317350, 'generated:000069', 'debit', 9030, '{"channel":"batch","generated":true,"sequence":2}');

-- Data key: generated:000070
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000070', 'iris.reed.70.42@example.test', 422832, 1, X'de56cd1d77f61324c1f739dc');

-- Data key: generated:000070
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317351, 'generated:000070', 'debit', 22198, '{"channel":"import","generated":true,"sequence":1}');

-- Data key: generated:000070
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317352, 'generated:000070', 'note', NULL, '{"channel":"mobile","generated":true,"sequence":2}');

-- Data key: generated:000071
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000071', 'lina.das.71.42@example.test', 414545, 1, X'43891f745eacbfac439561d2');

-- Data key: generated:000071
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317353, 'generated:000071', 'debit', 12272, '{"channel":"api","generated":true,"sequence":1}');

-- Data key: generated:000071
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317354, 'generated:000071', 'debit', 15549, '{"channel":"api","generated":true,"sequence":2}');

-- Data key: generated:000072
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000072', 'tariq.tan.72.42@example.test', 171969, 0, X'38a510a2d276e8b34da6681d');

-- Data key: generated:000072
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317355, 'generated:000072', 'credit', 1319, '{"channel":"console","generated":true,"sequence":1}');

-- Data key: generated:000072
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317356, 'generated:000072', 'note', NULL, '{"channel":"api","generated":true,"sequence":2}');

-- Data key: generated:000073
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000073', 'diego.ito.73.42@example.test', 465179, 1, X'63745eabf3beb2f28a6b96be');

-- Data key: generated:000073
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317357, 'generated:000073', 'debit', 13694, '{"channel":"web","generated":true,"sequence":1}');

-- Data key: generated:000073
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317358, 'generated:000073', 'credit', 16134, '{"channel":"mobile","generated":true,"sequence":2}');

-- Data key: generated:000074
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000074', 'nora.jones.74.42@example.test', 17152, 1, X'377171f33cda5c19fbaf5e8b');

-- Data key: generated:000074
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317359, 'generated:000074', 'note', NULL, '{"channel":"web","generated":true,"sequence":1}');

-- Data key: generated:000074
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317360, 'generated:000074', 'credit', 13146, '{"channel":"console","generated":true,"sequence":2}');

-- Data key: generated:000075
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000075', 'grace.evans.75.42@example.test', 497554, 1, X'7417a936a4a398f8050cc955');

-- Data key: generated:000075
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317361, 'generated:000075', 'credit', 4226, '{"channel":"mobile","generated":true,"sequence":1}');

-- Data key: generated:000075
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317362, 'generated:000075', 'credit', 18260, '{"channel":"batch","generated":true,"sequence":2}');

-- Data key: generated:000076
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000076', 'sofia.hassan.76.42@example.test', 426273, 0, X'54c625c9e6980046dbfb25fc');

-- Data key: generated:000076
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317363, 'generated:000076', 'credit', 8313, '{"channel":"batch","generated":true,"sequence":1}');

-- Data key: generated:000076
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317364, 'generated:000076', 'credit', 944, '{"channel":"batch","generated":true,"sequence":2}');

-- Data key: generated:000077
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000077', 'ada.martin.77.42@example.test', 414179, 1, X'9652042c430d20bd6b861dbe');

-- Data key: generated:000077
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317365, 'generated:000077', 'credit', 14789, '{"channel":"console","generated":true,"sequence":1}');

-- Data key: generated:000077
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317366, 'generated:000077', 'debit', 3673, '{"channel":"import","generated":true,"sequence":2}');

-- Data key: generated:000078
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000078', 'quinn.ito.78.42@example.test', 495670, 1, X'bac8e8dda8854d75a4f6070f');

-- Data key: generated:000078
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317367, 'generated:000078', 'note', NULL, '{"channel":"import","generated":true,"sequence":1}');

-- Data key: generated:000078
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317368, 'generated:000078', 'credit', 3637, '{"channel":"import","generated":true,"sequence":2}');

-- Data key: generated:000079
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000079', 'omar.das.79.42@example.test', 471474, 1, X'9b251020469fa2958cb65361');

-- Data key: generated:000079
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317369, 'generated:000079', 'note', NULL, '{"channel":"mobile","generated":true,"sequence":1}');

-- Data key: generated:000079
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317370, 'generated:000079', 'credit', 16665, '{"channel":"mobile","generated":true,"sequence":2}');

-- Data key: generated:000080
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000080', 'nora.evans.80.42@example.test', 415810, 1, X'daa7a6e0c48db8dd376e73e3');

-- Data key: generated:000080
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317371, 'generated:000080', 'credit', 11206, '{"channel":"import","generated":true,"sequence":1}');

-- Data key: generated:000080
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317372, 'generated:000080', 'credit', 24007, '{"channel":"api","generated":true,"sequence":2}');

-- Data key: generated:000081
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000081', 'kai.ortiz.81.42@example.test', 163852, 1, X'5ff427afec791117d415176e');

-- Data key: generated:000081
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317373, 'generated:000081', 'credit', 24308, '{"channel":"console","generated":true,"sequence":1}');

-- Data key: generated:000081
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317374, 'generated:000081', 'note', NULL, '{"channel":"mobile","generated":true,"sequence":2}');

-- Data key: generated:000082
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000082', 'amina.usman.82.42@example.test', 294433, 1, X'ab1f695adfaaf0c06cdeeab8');

-- Data key: generated:000082
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317375, 'generated:000082', 'credit', 9525, '{"channel":"mobile","generated":true,"sequence":1}');

-- Data key: generated:000082
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317376, 'generated:000082', 'credit', 3494, '{"channel":"mobile","generated":true,"sequence":2}');

-- Data key: generated:000083
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000083', 'quinn.hassan.83.42@example.test', 81123, 1, X'39d81b59d88e5e1dc3479239');

-- Data key: generated:000083
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317377, 'generated:000083', 'note', NULL, '{"channel":"mobile","generated":true,"sequence":1}');

-- Data key: generated:000083
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317378, 'generated:000083', 'note', NULL, '{"channel":"mobile","generated":true,"sequence":2}');

-- Data key: generated:000084
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000084', 'tariq.tan.84.42@example.test', 13764, 1, X'a8d4b144072e45b3c34feb56');

-- Data key: generated:000084
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317379, 'generated:000084', 'credit', 6044, '{"channel":"batch","generated":true,"sequence":1}');

-- Data key: generated:000084
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317380, 'generated:000084', 'debit', 13232, '{"channel":"api","generated":true,"sequence":2}');

-- Data key: generated:000085
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000085', 'elena.baker.85.42@example.test', 48107, 1, X'37606b7457285e4fb853c6f1');

-- Data key: generated:000085
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317381, 'generated:000085', 'debit', 2883, '{"channel":"api","generated":true,"sequence":1}');

-- Data key: generated:000085
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317382, 'generated:000085', 'credit', 24821, '{"channel":"mobile","generated":true,"sequence":2}');

-- Data key: generated:000086
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000086', 'amina.das.86.42@example.test', 142660, 1, X'6c7c9b716a4537c1831d586e');

-- Data key: generated:000086
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317383, 'generated:000086', 'credit', 22323, '{"channel":"web","generated":true,"sequence":1}');

-- Data key: generated:000086
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317384, 'generated:000086', 'debit', 17366, '{"channel":"web","generated":true,"sequence":2}');

-- Data key: generated:000087
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000087', 'jamal.costa.87.42@example.test', 115615, 1, X'990e01344df236c423c3414a');

-- Data key: generated:000087
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317385, 'generated:000087', 'credit', 353, '{"channel":"import","generated":true,"sequence":1}');

-- Data key: generated:000087
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317386, 'generated:000087', 'debit', 5857, '{"channel":"batch","generated":true,"sequence":2}');

-- Data key: generated:000088
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000088', 'mateo.tan.88.42@example.test', 368926, 1, X'8fd5abce5a1265dcbd0a6f04');

-- Data key: generated:000088
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317387, 'generated:000088', 'credit', 2651, '{"channel":"console","generated":true,"sequence":1}');

-- Data key: generated:000088
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317388, 'generated:000088', 'debit', 18889, '{"channel":"import","generated":true,"sequence":2}');

-- Data key: generated:000089
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000089', 'nora.khan.89.42@example.test', 60384, 1, X'f7532bcdf29e75d4b0eb5c16');

-- Data key: generated:000089
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317389, 'generated:000089', 'credit', 3568, '{"channel":"batch","generated":true,"sequence":1}');

-- Data key: generated:000089
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317390, 'generated:000089', 'credit', 13221, '{"channel":"mobile","generated":true,"sequence":2}');

-- Data key: generated:000090
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000090', 'chen.ng.90.42@example.test', 456405, 1, X'563855c72b1382a21d878231');

-- Data key: generated:000090
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317391, 'generated:000090', 'note', NULL, '{"channel":"console","generated":true,"sequence":1}');

-- Data key: generated:000090
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317392, 'generated:000090', 'credit', 21249, '{"channel":"batch","generated":true,"sequence":2}');

-- Data key: generated:000091
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000091', 'hiro.evans.91.42@example.test', 76778, 1, X'2c9a27c2c2a7132df3c5a07e');

-- Data key: generated:000091
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317393, 'generated:000091', 'credit', 18572, '{"channel":"mobile","generated":true,"sequence":1}');

-- Data key: generated:000091
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317394, 'generated:000091', 'credit', 18597, '{"channel":"web","generated":true,"sequence":2}');

-- Data key: generated:000092
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000092', 'tariq.lee.92.42@example.test', 453114, 0, X'502670117871a14dcb46970e');

-- Data key: generated:000092
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317395, 'generated:000092', 'credit', 2531, '{"channel":"console","generated":true,"sequence":1}');

-- Data key: generated:000092
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317396, 'generated:000092', 'credit', 1332, '{"channel":"api","generated":true,"sequence":2}');

-- Data key: generated:000093
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000093', 'lina.khan.93.42@example.test', 40229, 1, X'fada179d98816276948df4ca');

-- Data key: generated:000093
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317397, 'generated:000093', 'debit', 1444, '{"channel":"import","generated":true,"sequence":1}');

-- Data key: generated:000093
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317398, 'generated:000093', 'note', NULL, '{"channel":"mobile","generated":true,"sequence":2}');

-- Data key: generated:000094
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000094', 'grace.lee.94.42@example.test', 317197, 1, X'26f50f731acfe6d657fbb615');

-- Data key: generated:000094
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317399, 'generated:000094', 'debit', 5754, '{"channel":"api","generated":true,"sequence":1}');

-- Data key: generated:000094
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317400, 'generated:000094', 'credit', 14444, '{"channel":"import","generated":true,"sequence":2}');

-- Data key: generated:000095
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000095', 'quinn.singh.95.42@example.test', 319713, 1, X'5fea486368c656ad990dcaa1');

-- Data key: generated:000095
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317401, 'generated:000095', 'debit', 2259, '{"channel":"console","generated":true,"sequence":1}');

-- Data key: generated:000095
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317402, 'generated:000095', 'credit', 22329, '{"channel":"import","generated":true,"sequence":2}');

-- Data key: generated:000096
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000096', 'jamal.jones.96.42@example.test', 379444, 0, X'f6e89adf26551495a924ea59');

-- Data key: generated:000096
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317403, 'generated:000096', 'credit', 21597, '{"channel":"web","generated":true,"sequence":1}');

-- Data key: generated:000096
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317404, 'generated:000096', 'debit', 4325, '{"channel":"mobile","generated":true,"sequence":2}');

-- Data key: generated:000097
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000097', 'chen.khan.97.42@example.test', 293036, 1, X'ca54d020abb3d4f2bdffafe9');

-- Data key: generated:000097
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317405, 'generated:000097', 'debit', 21268, '{"channel":"web","generated":true,"sequence":1}');

-- Data key: generated:000097
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317406, 'generated:000097', 'credit', 11956, '{"channel":"api","generated":true,"sequence":2}');

-- Data key: generated:000098
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000098', 'lina.khan.98.42@example.test', 94509, 0, X'57f2c47c3139ff2327134bd8');

-- Data key: generated:000098
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317407, 'generated:000098', 'debit', 16735, '{"channel":"mobile","generated":true,"sequence":1}');

-- Data key: generated:000098
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317408, 'generated:000098', 'note', NULL, '{"channel":"mobile","generated":true,"sequence":2}');

-- Data key: generated:000099
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000099', 'amina.lee.99.42@example.test', 459482, 1, X'21986027292ed4b1c59fcfe7');

-- Data key: generated:000099
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317409, 'generated:000099', 'credit', 14441, '{"channel":"api","generated":true,"sequence":1}');

-- Data key: generated:000099
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317410, 'generated:000099', 'credit', 22263, '{"channel":"web","generated":true,"sequence":2}');

-- Data key: generated:000100
INSERT INTO console_test_accounts (account_id, email, balance_cents, active, profile) VALUES ('generated:000100', 'hiro.patel.100.42@example.test', 320127, 1, X'bfc8723b883d4ff7cfc878e7');

-- Data key: generated:000100
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317411, 'generated:000100', 'note', NULL, '{"channel":"console","generated":true,"sequence":1}');

-- Data key: generated:000100
INSERT INTO console_test_events (event_id, account_id, kind, amount_cents, metadata) VALUES (3746317412, 'generated:000100', 'debit', 18791, '{"channel":"import","generated":true,"sequence":2}');

-- Data key: generated:000060
UPDATE console_test_accounts SET email = 'iris.baker.60.42.update1@example.test', balance_cents = MAX(0, balance_cents + (7748)), active = 1, profile = X'6180876bf729d133cd9a23df' WHERE account_id = 'generated:000060';

-- Data key: generated:000033
UPDATE console_test_accounts SET email = 'diego.patel.33.42.update2@example.test', balance_cents = MAX(0, balance_cents + (5505)), active = 0, profile = X'7bdf5f8def1ab6d884d91f48' WHERE account_id = 'generated:000033';

-- Data key: generated:000011
UPDATE console_test_accounts SET email = 'chen.das.11.42.update3@example.test', balance_cents = MAX(0, balance_cents + (-531)), active = 0, profile = X'73e78325d46f17f2e938d173' WHERE account_id = 'generated:000011';

-- Data key: generated:000045
UPDATE console_test_accounts SET email = 'iris.tan.45.42.update4@example.test', balance_cents = MAX(0, balance_cents + (1797)), active = 0, profile = X'0d65805f3c62fe145f390751' WHERE account_id = 'generated:000045';

-- Data key: generated:000013
UPDATE console_test_accounts SET email = 'amina.ito.13.42.update5@example.test', balance_cents = MAX(0, balance_cents + (7965)), active = 1, profile = X'25230949ead478b2d423c2b4' WHERE account_id = 'generated:000013';

-- Data key: generated:000061
UPDATE console_test_accounts SET email = 'hiro.usman.61.42.update6@example.test', balance_cents = MAX(0, balance_cents + (5081)), active = 1, profile = X'01e714044137d5268cf0ba9b' WHERE account_id = 'generated:000061';

-- Data key: generated:000068
UPDATE console_test_accounts SET email = 'amina.vega.68.42.update7@example.test', balance_cents = MAX(0, balance_cents + (-3179)), active = 1, profile = X'c6493c4d1f0c3d6ba3cb9f75' WHERE account_id = 'generated:000068';

-- Data key: generated:000009
UPDATE console_test_accounts SET email = 'jamal.ortiz.9.42.update8@example.test', balance_cents = MAX(0, balance_cents + (8708)), active = 0, profile = X'e77f988904a183933db7244a' WHERE account_id = 'generated:000009';

-- Data key: generated:000055
UPDATE console_test_accounts SET email = 'iris.garcia.55.42.update9@example.test', balance_cents = MAX(0, balance_cents + (5073)), active = 0, profile = X'5a3d926a2faaab1586f95c11' WHERE account_id = 'generated:000055';

-- Data key: generated:000068
UPDATE console_test_accounts SET email = 'amina.vega.68.42.update10@example.test', balance_cents = MAX(0, balance_cents + (1397)), active = 0, profile = X'df780ba263fb5f40bf045bc9' WHERE account_id = 'generated:000068';

-- Data key: generated:000009
UPDATE console_test_accounts SET email = 'jamal.ortiz.9.42.update11@example.test', balance_cents = MAX(0, balance_cents + (-1050)), active = 1, profile = X'bba8a01ac594bcc155220b5a' WHERE account_id = 'generated:000009';

-- Data key: generated:000070
UPDATE console_test_accounts SET email = 'iris.reed.70.42.update12@example.test', balance_cents = MAX(0, balance_cents + (8328)), active = 1, profile = X'a42cd4c7af76fbb27aa12ecf' WHERE account_id = 'generated:000070';

-- Data key: generated:000018
UPDATE console_test_accounts SET email = 'farah.das.18.42.update13@example.test', balance_cents = MAX(0, balance_cents + (6731)), active = 0, profile = X'c6f175094b330bca33e20a50' WHERE account_id = 'generated:000018';

-- Data key: generated:000040
UPDATE console_test_accounts SET email = 'elena.ito.40.42.update14@example.test', balance_cents = MAX(0, balance_cents + (8359)), active = 1, profile = X'8b794009c0a530495bdcc70c' WHERE account_id = 'generated:000040';

-- Data key: generated:000084
UPDATE console_test_accounts SET email = 'tariq.tan.84.42.update15@example.test', balance_cents = MAX(0, balance_cents + (-524)), active = 1, profile = X'1fcc5e6fe366be70e5f46256' WHERE account_id = 'generated:000084';

-- Data key: generated:000024
UPDATE console_test_accounts SET email = 'grace.ito.24.42.update16@example.test', balance_cents = MAX(0, balance_cents + (6338)), active = 1, profile = X'7f5eeccc8444cd15ba6c146e' WHERE account_id = 'generated:000024';

-- Data key: generated:000078
UPDATE console_test_accounts SET email = 'quinn.ito.78.42.update17@example.test', balance_cents = MAX(0, balance_cents + (3937)), active = 0, profile = X'4b521a1453a94b4e729ab76d' WHERE account_id = 'generated:000078';

-- Data key: generated:000022
UPDATE console_test_accounts SET email = 'priya.jones.22.42.update18@example.test', balance_cents = MAX(0, balance_cents + (759)), active = 1, profile = X'720abadee95a9dff6f46a3fa' WHERE account_id = 'generated:000022';

-- Data key: generated:000008
UPDATE console_test_accounts SET email = 'chen.ng.8.42.update19@example.test', balance_cents = MAX(0, balance_cents + (5996)), active = 0, profile = X'a3675d83cdbfad28f30724d9' WHERE account_id = 'generated:000008';

-- Data key: generated:000078
UPDATE console_test_accounts SET email = 'quinn.ito.78.42.update20@example.test', balance_cents = MAX(0, balance_cents + (-4430)), active = 1, profile = X'20113cc7a55d5c62f391089a' WHERE account_id = 'generated:000078';

-- Data key: generated:000020
UPDATE console_test_accounts SET email = 'iris.fischer.20.42.update21@example.test', balance_cents = MAX(0, balance_cents + (1077)), active = 1, profile = X'5f71c3139223875d6550a647' WHERE account_id = 'generated:000020';

-- Data key: generated:000032
UPDATE console_test_accounts SET email = 'grace.ng.32.42.update22@example.test', balance_cents = MAX(0, balance_cents + (-4576)), active = 0, profile = X'bc2f7f8463e98f1e43c642b4' WHERE account_id = 'generated:000032';

-- Data key: generated:000058
UPDATE console_test_accounts SET email = 'farah.khan.58.42.update23@example.test', balance_cents = MAX(0, balance_cents + (5024)), active = 0, profile = X'49b1eaff7d331f22da12732c' WHERE account_id = 'generated:000058';

-- Data key: generated:000092
UPDATE console_test_accounts SET email = 'tariq.lee.92.42.update24@example.test', balance_cents = MAX(0, balance_cents + (-3562)), active = 1, profile = X'cfaef7d8fc51aa58b5108c8a' WHERE account_id = 'generated:000092';

-- Data key: generated:000038
UPDATE console_test_accounts SET email = 'sofia.baker.38.42.update25@example.test', balance_cents = MAX(0, balance_cents + (8948)), active = 1, profile = X'28b6b5edb3a32ccb5c82391f' WHERE account_id = 'generated:000038';

-- Data key: generated:000026
UPDATE console_test_accounts SET email = 'jamal.ito.26.42.update26@example.test', balance_cents = MAX(0, balance_cents + (-1122)), active = 0, profile = X'ca7e065c8d925e77cdfb8d21' WHERE account_id = 'generated:000026';

-- Data key: generated:000079
UPDATE console_test_accounts SET email = 'omar.das.79.42.update27@example.test', balance_cents = MAX(0, balance_cents + (-3923)), active = 0, profile = X'4f65ffb7b87a8669c468d293' WHERE account_id = 'generated:000079';

-- Data key: generated:000010
UPDATE console_test_accounts SET email = 'tariq.hassan.10.42.update28@example.test', balance_cents = MAX(0, balance_cents + (194)), active = 0, profile = X'a4127377ae845820e0d4c78d' WHERE account_id = 'generated:000010';

-- Data key: generated:000082
UPDATE console_test_accounts SET email = 'amina.usman.82.42.update29@example.test', balance_cents = MAX(0, balance_cents + (7579)), active = 0, profile = X'f7216e80e9de0ed41f84274d' WHERE account_id = 'generated:000082';

-- Data key: generated:000022
UPDATE console_test_accounts SET email = 'priya.jones.22.42.update30@example.test', balance_cents = MAX(0, balance_cents + (286)), active = 0, profile = X'efb53958f2f084e548d81440' WHERE account_id = 'generated:000022';

-- Data key: generated:000026
UPDATE console_test_accounts SET email = 'jamal.ito.26.42.update31@example.test', balance_cents = MAX(0, balance_cents + (-2950)), active = 1, profile = X'a04d9d881780a42b97f19427' WHERE account_id = 'generated:000026';

-- Data key: generated:000022
UPDATE console_test_accounts SET email = 'priya.jones.22.42.update32@example.test', balance_cents = MAX(0, balance_cents + (8804)), active = 1, profile = X'ec900ad3dd07140bf2a4c593' WHERE account_id = 'generated:000022';

-- Data key: generated:000034
UPDATE console_test_accounts SET email = 'omar.garcia.34.42.update33@example.test', balance_cents = MAX(0, balance_cents + (7565)), active = 0, profile = X'926a9ea3077fe3a08b4aa4f4' WHERE account_id = 'generated:000034';

-- Data key: generated:000039
UPDATE console_test_accounts SET email = 'chen.reed.39.42.update34@example.test', balance_cents = MAX(0, balance_cents + (-988)), active = 1, profile = X'ceceaf674c7412b00f28706a' WHERE account_id = 'generated:000039';

-- Data key: generated:000062
UPDATE console_test_accounts SET email = 'quinn.usman.62.42.update35@example.test', balance_cents = MAX(0, balance_cents + (-1658)), active = 1, profile = X'579b2450dcb751bbfcdc58f9' WHERE account_id = 'generated:000062';

-- Data key: generated:000052
UPDATE console_test_accounts SET email = 'chen.ito.52.42.update36@example.test', balance_cents = MAX(0, balance_cents + (7462)), active = 0, profile = X'5e838f1b513d771f44733f24' WHERE account_id = 'generated:000052';

-- Data key: generated:000013
UPDATE console_test_accounts SET email = 'amina.ito.13.42.update37@example.test', balance_cents = MAX(0, balance_cents + (-246)), active = 0, profile = X'f262dd9d6b3ff6dde628d053' WHERE account_id = 'generated:000013';

-- Data key: generated:000074
UPDATE console_test_accounts SET email = 'nora.jones.74.42.update38@example.test', balance_cents = MAX(0, balance_cents + (-1890)), active = 1, profile = X'c3287ffe83777fe14e7f0517' WHERE account_id = 'generated:000074';

-- Data key: generated:000051
UPDATE console_test_accounts SET email = 'grace.hassan.51.42.update39@example.test', balance_cents = MAX(0, balance_cents + (-1056)), active = 1, profile = X'37955a0c0c487e98e1d7a7ac' WHERE account_id = 'generated:000051';

-- Data key: generated:000061
UPDATE console_test_accounts SET email = 'hiro.usman.61.42.update40@example.test', balance_cents = MAX(0, balance_cents + (3793)), active = 1, profile = X'02d81b6e22e143ba5dc3675d' WHERE account_id = 'generated:000061';

