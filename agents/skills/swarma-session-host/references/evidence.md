# Source evidence

Baseline commit: `e39bf903049fa49b52b7ada6ba269ed6e34eede5`. All locators below refer to that immutable source, not a moving main branch.
Git blob SHA-1 identifies the complete file; line ranges identify the inspected claim evidence.
**Every test cited below was inspected as source, not executed for this skill.** Documentation constraints and commands are guidance, not execution evidence.
C00 includes the workflow limitation imposed on this skill: G07, grant consumption, live runtime and root custody are unavailable. Those labels do not name newly supported Host APIs.
The absence of an external plugin/activation command is scoped to the inspected tracked entrypoints, internal interfaces and module inventory; it is not a statement about another component or future branch.

## C00

Unavailable authority and runtime routes; G07/root custody are task scope limits, not implemented interfaces.

- [internal/sessionhost/server.go:89–126](../../../../internal/sessionhost/server.go#L89) — blob `addfbf5a889db33c3b6e0fecf5ed62ddb86a26fa`; source inspected.
- [internal/sessionhost/control.go:3–23](../../../../internal/sessionhost/control.go#L3) — blob `c6acb68ace48543a8d12303fe8d09bbf92ad2267`; source inspected.
- [internal/sessionhost/notification_manager.go:14–39](../../../../internal/sessionhost/notification_manager.go#L14) — blob `469e95f735fcfaa05ecf6a646a6f889a5c82ec5e`; source inspected.
- [README.md:21–32](../../../../README.md#L21) — blob `bfa6847c286317fc28db7e6229dd569623dfcaea`; source inspected.
- [UNRESOLVED_QUESTIONS.md:3–45](../../../../UNRESOLVED_QUESTIONS.md#L3) — blob `86cc26aa2e74d2f0496157a8f815e0c912195c6c`; source inspected.

## C01

Host/runner responsibilities, implemented adapters and schematic Claude/Muse.

- [README.md:3–35](../../../../README.md#L3) — blob `bfa6847c286317fc28db7e6229dd569623dfcaea`; source inspected.
- [internal/adapterprotocol/contract_test.go:15–33](../../../../internal/adapterprotocol/contract_test.go#L15) — blob `72e93f4cf909d628df12e3aea3cabf61e1204679`; test source inspected; not executed.

## C02

Module/toolchain prerequisites and unsupported platform proof.

- [go.mod:1–3](../../../../go.mod#L1) — blob `445766ad22ab92c4b82a5962ad8a5d98971a1150`; source inspected.
- [README.md:74–87](../../../../README.md#L74) — blob `bfa6847c286317fc28db7e6229dd569623dfcaea`; source inspected.
- [internal/admission/process_unsupported.go:1–12](../../../../internal/admission/process_unsupported.go#L1) — blob `4d1e959d9d277c555233ae7336ff165cbc787c51`; source inspected.
- [internal/localipc/identity_linux.go:8–12](../../../../internal/localipc/identity_linux.go#L8) — blob `9d2af85dbd3a60d043de9c326d9341d9d848aa04`; source inspected.
- [docs/prototype-m1.md:5–9](../../../../docs/prototype-m1.md#L5) — blob `67f47adbe1f83912526eb370d56325eb1218ea99`; source inspected.

## C03

Three binary entrypoints, build/vet/race recipes and synthetic smoke behavior.

- [README.md:74–98](../../../../README.md#L74) — blob `bfa6847c286317fc28db7e6229dd569623dfcaea`; source inspected.
- [cmd/swarma-session-host/main.go:25–37](../../../../cmd/swarma-session-host/main.go#L25) — blob `819e36a5fa1e402959dbc81ecc040a1c754eb40f`; source inspected.
- [cmd/swarma-session-runner/main.go:16–42](../../../../cmd/swarma-session-runner/main.go#L16) — blob `8bd45302aa096d2df4da8e9e77c19efa921c6075`; source inspected.
- [cmd/session-fixture/main.go:11–18](../../../../cmd/session-fixture/main.go#L11) — blob `1545ea090483e85851baaacbbfb0c2c32f85d4c6`; source inspected.
- [.scripts/prototype-smoke.py:36–143](../../../../.scripts/prototype-smoke.py#L36) — blob `f6cc8c44871eb98fd5b1cb29349a745e3f61c714`; source inspected.

## C04

Explicit runner plan, bounded validation and foreground owned child creation.

- [internal/runner/runner.go:32–45](../../../../internal/runner/runner.go#L32) — blob `e192af27a26f9e8b264721e665bf254aea683d2d`; source inspected.
- [internal/runner/runner.go:146–204](../../../../internal/runner/runner.go#L146) — blob `e192af27a26f9e8b264721e665bf254aea683d2d`; source inspected.
- [cmd/swarma-session-runner/main.go:16–42](../../../../cmd/swarma-session-runner/main.go#L16) — blob `8bd45302aa096d2df4da8e9e77c19efa921c6075`; source inspected.
- [docs/prototype-m1.md:123–148](../../../../docs/prototype-m1.md#L123) — blob `67f47adbe1f83912526eb370d56325eb1218ea99`; source inspected.

## C05

Synthetic CLI wiring, explicit idle evidence and opaque bounded selections.

- [cmd/swarma-session-host/main.go:39–85](../../../../cmd/swarma-session-host/main.go#L39) — blob `819e36a5fa1e402959dbc81ecc040a1c754eb40f`; source inspected.
- [cmd/session-fixture/main.go:11–18](../../../../cmd/session-fixture/main.go#L11) — blob `1545ea090483e85851baaacbbfb0c2c32f85d4c6`; source inspected.
- [internal/sessionhost/host.go:34–78](../../../../internal/sessionhost/host.go#L34) — blob `2f9102997bb0d124fc3db550a1fbf3f7bd004152`; source inspected.
- [internal/sessionhost/host.go:154–158](../../../../internal/sessionhost/host.go#L154) — blob `2f9102997bb0d124fc3db550a1fbf3f7bd004152`; source inspected.
- [docs/prototype-m1.md:131–166](../../../../docs/prototype-m1.md#L131) — blob `67f47adbe1f83912526eb370d56325eb1218ea99`; source inspected.
- [internal/sessionhost/host_test.go:111–150](../../../../internal/sessionhost/host_test.go#L111) — blob `504057ea31015088254d4142c108c42bb3f10fb4`; test source inspected; not executed.

## C06

Host control request shape, supported/refused operations, mutation selectors and request exit behavior.

- [internal/sessionhost/server.go:16–28](../../../../internal/sessionhost/server.go#L16) — blob `addfbf5a889db33c3b6e0fecf5ed62ddb86a26fa`; source inspected.
- [internal/sessionhost/server.go:89–126](../../../../internal/sessionhost/server.go#L89) — blob `addfbf5a889db33c3b6e0fecf5ed62ddb86a26fa`; source inspected.
- [cmd/swarma-session-host/main.go:108–129](../../../../cmd/swarma-session-host/main.go#L108) — blob `819e36a5fa1e402959dbc81ecc040a1c754eb40f`; source inspected.
- [internal/sessionhost/server_test.go:13–67](../../../../internal/sessionhost/server_test.go#L13) — blob `2d1e93e55678d7e217116b4eb29d7dbd36fab840`; test source inspected; not executed.

## C07

Private modes, bounded closed transport, occupied paths and same-UID limitation.

- [internal/localipc/transport.go:16–80](../../../../internal/localipc/transport.go#L16) — blob `1f4a5087e99b3869d358e52bd68ef913d3fbc557`; source inspected.
- [internal/localipc/transport_test.go:13–92](../../../../internal/localipc/transport_test.go#L13) — blob `8223316fa5cd779fbd27acae28b342bbf4d59a36`; test source inspected; not executed.
- [internal/sessionhost/server.go:30–31](../../../../internal/sessionhost/server.go#L30) — blob `addfbf5a889db33c3b6e0fecf5ed62ddb86a26fa`; source inspected.
- [internal/sessionhost/server.go:74–84](../../../../internal/sessionhost/server.go#L74) — blob `addfbf5a889db33c3b6e0fecf5ed62ddb86a26fa`; source inspected.
- [internal/sessionhost/store.go:21–50](../../../../internal/sessionhost/store.go#L21) — blob `2c83a8d99515617986cfb06a92cc8fbe995e89bb`; source inspected.
- [docs/prototype-m1.md:41–46](../../../../docs/prototype-m1.md#L41) — blob `67f47adbe1f83912526eb370d56325eb1218ea99`; source inspected.

## C08

Intent/deduplication, full fixture acknowledgement and separate native acceptance/completion.

- [internal/sessionhost/host.go:418–459](../../../../internal/sessionhost/host.go#L418) — blob `2f9102997bb0d124fc3db550a1fbf3f7bd004152`; source inspected.
- [internal/sessionhost/host.go:553–630](../../../../internal/sessionhost/host.go#L553) — blob `2f9102997bb0d124fc3db550a1fbf3f7bd004152`; source inspected.
- [internal/fixtureadapters/adapters.go:35–85](../../../../internal/fixtureadapters/adapters.go#L35) — blob `8326784a138340989e5ef0a47c8a37e302778c96`; source inspected.
- [internal/fixtureadapters/adapters_test.go:11–67](../../../../internal/fixtureadapters/adapters_test.go#L11) — blob `5e1e77bad6740385f02a2bcab02c44031cff1e78`; test source inspected; not executed.
- [internal/sessionhost/host_test.go:111–150](../../../../internal/sessionhost/host_test.go#L111) — blob `504057ea31015088254d4142c108c42bb3f10fb4`; test source inspected; not executed.
- [internal/sessionhost/protocol.go:181–234](../../../../internal/sessionhost/protocol.go#L181) — blob `953966db988961d33f8210f04d66372ddad28215`; source inspected.
- [docs/prototype-m1.md:150–160](../../../../docs/prototype-m1.md#L150) — blob `67f47adbe1f83912526eb370d56325eb1218ea99`; source inspected.
- [docs/work-reference.md:111–114](../../../../docs/work-reference.md#L111) — blob `29be083d22dc1609f1d310177191d4112541ee17`; source inspected.

## C09

Retained uncertainty, storage refusals and replacement without write replay.

- [internal/sessionhost/host.go:210–224](../../../../internal/sessionhost/host.go#L210) — blob `2f9102997bb0d124fc3db550a1fbf3f7bd004152`; source inspected.
- [internal/sessionhost/host.go:262–345](../../../../internal/sessionhost/host.go#L262) — blob `2f9102997bb0d124fc3db550a1fbf3f7bd004152`; source inspected.
- [internal/sessionhost/host.go:462–493](../../../../internal/sessionhost/host.go#L462) — blob `2f9102997bb0d124fc3db550a1fbf3f7bd004152`; source inspected.
- [internal/sessionhost/store.go:53–141](../../../../internal/sessionhost/store.go#L53) — blob `2c83a8d99515617986cfb06a92cc8fbe995e89bb`; source inspected.
- [internal/sessionhost/protocol.go:300–340](../../../../internal/sessionhost/protocol.go#L300) — blob `953966db988961d33f8210f04d66372ddad28215`; source inspected.
- [internal/sessionhost/host_test.go:186–263](../../../../internal/sessionhost/host_test.go#L186) — blob `504057ea31015088254d4142c108c42bb3f10fb4`; test source inspected; not executed.
- [internal/sessionhost/host_test.go:338–386](../../../../internal/sessionhost/host_test.go#L338) — blob `504057ea31015088254d4142c108c42bb3f10fb4`; test source inspected; not executed.
- [internal/sessionhost/protocol_test.go:327–498](../../../../internal/sessionhost/protocol_test.go#L327) — blob `0fe29282152b6e141fefd72c4c8fb7906da2d09d`; test source inspected; not executed.
- [docs/prototype-m1.md:123–129](../../../../docs/prototype-m1.md#L123) — blob `67f47adbe1f83912526eb370d56325eb1218ea99`; source inspected.
- [docs/work-reference.md:95–99](../../../../docs/work-reference.md#L95) — blob `29be083d22dc1609f1d310177191d4112541ee17`; source inspected.

## C10

Explicit idle, sticky security stop and inability to retract issued effects.

- [internal/sessionhost/host.go:462–531](../../../../internal/sessionhost/host.go#L462) — blob `2f9102997bb0d124fc3db550a1fbf3f7bd004152`; source inspected.
- [internal/sessionhost/host.go:553–596](../../../../internal/sessionhost/host.go#L553) — blob `2f9102997bb0d124fc3db550a1fbf3f7bd004152`; source inspected.
- [internal/sessionhost/eligibility_test.go:21–122](../../../../internal/sessionhost/eligibility_test.go#L21) — blob `a46f0de40e9afc5c4bec2046c12a5c19fc7475ac`; test source inspected; not executed.

## C11

Pinned Codex construction, qualified capability subset and refusal before effects.

- [cmd/swarma-session-host/main.go:43–85](../../../../cmd/swarma-session-host/main.go#L43) — blob `819e36a5fa1e402959dbc81ecc040a1c754eb40f`; source inspected.
- [internal/codexadapter/driver.go:39–85](../../../../internal/codexadapter/driver.go#L39) — blob `63f4330284d0c55289750c8bfa198260c39a0a49`; source inspected.
- [internal/codexadapter/driver.go:193–233](../../../../internal/codexadapter/driver.go#L193) — blob `63f4330284d0c55289750c8bfa198260c39a0a49`; source inspected.
- [internal/codexadapter/driver_test.go:49–144](../../../../internal/codexadapter/driver_test.go#L49) — blob `483b9fb95d737dbfc3425856afca157d2884f7e2`; test source inspected; not executed.
- [docs/prototype-m2.md:164–183](../../../../docs/prototype-m2.md#L164) — blob `10999233cd595b8e32059933a745bfc173367b9a`; source inspected.
- [docs/work-reference.md:8–12](../../../../docs/work-reference.md#L8) — blob `29be083d22dc1609f1d310177191d4112541ee17`; source inspected.

## C12

Separate work mode/policy, immutable checkpoint binding and fixed reference rendering.

- [cmd/swarma-session-host/main.go:62–79](../../../../cmd/swarma-session-host/main.go#L62) — blob `819e36a5fa1e402959dbc81ecc040a1c754eb40f`; source inspected.
- [internal/codexadapter/driver.go:72–129](../../../../internal/codexadapter/driver.go#L72) — blob `63f4330284d0c55289750c8bfa198260c39a0a49`; source inspected.
- [internal/codexadapter/driver.go:207–245](../../../../internal/codexadapter/driver.go#L207) — blob `63f4330284d0c55289750c8bfa198260c39a0a49`; source inspected.
- [internal/sessionhost/host.go:180–224](../../../../internal/sessionhost/host.go#L180) — blob `2f9102997bb0d124fc3db550a1fbf3f7bd004152`; source inspected.
- [internal/sessionhost/host.go:238–249](../../../../internal/sessionhost/host.go#L238) — blob `2f9102997bb0d124fc3db550a1fbf3f7bd004152`; source inspected.
- [internal/codexadapter/protocol.go:118–136](../../../../internal/codexadapter/protocol.go#L118) — blob `2c682c65be2bb616d2a64c7c35d67b0f126b5f29`; source inspected.
- [internal/codexadapter/protocol.go:195–223](../../../../internal/codexadapter/protocol.go#L195) — blob `2c682c65be2bb616d2a64c7c35d67b0f126b5f29`; source inspected.
- [internal/codexadapter/work_reference_test.go:41–150](../../../../internal/codexadapter/work_reference_test.go#L41) — blob `322977905b4bd7391e06972bdf6473a1d94a8ac4`; test source inspected; not executed.
- [internal/codexadapter/work_reference_test.go:152–238](../../../../internal/codexadapter/work_reference_test.go#L152) — blob `322977905b4bd7391e06972bdf6473a1d94a8ac4`; test source inspected; not executed.
- [docs/work-reference.md:19–27](../../../../docs/work-reference.md#L19) — blob `29be083d22dc1609f1d310177191d4112541ee17`; source inspected.
- [docs/work-reference.md:64–92](../../../../docs/work-reference.md#L64) — blob `29be083d22dc1609f1d310177191d4112541ee17`; source inspected.

## C13

Owner-only attention without approval response, unsupported goals and unqualified optional capabilities.

- [internal/codexadapter/protocol.go:165–175](../../../../internal/codexadapter/protocol.go#L165) — blob `2c682c65be2bb616d2a64c7c35d67b0f126b5f29`; source inspected.
- [internal/codexadapter/driver.go:146–161](../../../../internal/codexadapter/driver.go#L146) — blob `63f4330284d0c55289750c8bfa198260c39a0a49`; source inspected.
- [internal/codexadapter/driver_test.go:94–144](../../../../internal/codexadapter/driver_test.go#L94) — blob `483b9fb95d737dbfc3425856afca157d2884f7e2`; test source inspected; not executed.
- [internal/codexadapter/driver_test.go:157–183](../../../../internal/codexadapter/driver_test.go#L157) — blob `483b9fb95d737dbfc3425856afca157d2884f7e2`; test source inspected; not executed.
- [internal/sessionhost/control.go:3–23](../../../../internal/sessionhost/control.go#L3) — blob `c6acb68ace48543a8d12303fe8d09bbf92ad2267`; source inspected.
- [internal/sessionhost/server_test.go:41–65](../../../../internal/sessionhost/server_test.go#L41) — blob `2d1e93e55678d7e217116b4eb29d7dbd36fab840`; test source inspected; not executed.

## C14

Retained output and journal bounds, durable complete-frame consumption and terminal fences.

- [internal/runner/runner.go:23–30](../../../../internal/runner/runner.go#L23) — blob `e192af27a26f9e8b264721e665bf254aea683d2d`; source inspected.
- [internal/runner/runner.go:221–275](../../../../internal/runner/runner.go#L221) — blob `e192af27a26f9e8b264721e665bf254aea683d2d`; source inspected.
- [internal/runner/runner.go:560–617](../../../../internal/runner/runner.go#L560) — blob `e192af27a26f9e8b264721e665bf254aea683d2d`; source inspected.
- [internal/adapterprotocol/contract.go:10–16](../../../../internal/adapterprotocol/contract.go#L10) — blob `521829cbf54cbf2d668f4630cbe12a215c1a94dd`; source inspected.
- [internal/sessionhost/host.go:20–20](../../../../internal/sessionhost/host.go#L20) — blob `2f9102997bb0d124fc3db550a1fbf3f7bd004152`; source inspected.
- [internal/sessionhost/store.go:98–102](../../../../internal/sessionhost/store.go#L98) — blob `2c83a8d99515617986cfb06a92cc8fbe995e89bb`; source inspected.
- [internal/sessionhost/protocol.go:360–436](../../../../internal/sessionhost/protocol.go#L360) — blob `953966db988961d33f8210f04d66372ddad28215`; source inspected.
- [internal/runner/consumed_output_test.go:32–123](../../../../internal/runner/consumed_output_test.go#L32) — blob `59c24080e12b01d7e6ab0c4cd3e0d767754e011f`; test source inspected; not executed.
- [internal/sessionhost/consumed_output_test.go:173–212](../../../../internal/sessionhost/consumed_output_test.go#L173) — blob `b36844eb329368bf296cb052058520e8a51df1d1`; test source inspected; not executed.
- [internal/sessionhost/protocol_terminal_reply_test.go:22–142](../../../../internal/sessionhost/protocol_terminal_reply_test.go#L22) — blob `21e63b9282ca4a2d48d8fb3372c6b640df56480d`; test source inspected; not executed.

## C15

Host close versus explicit stop; original direct-child authority and cooperative uncertainty.

- [internal/sessionhost/host.go:633–672](../../../../internal/sessionhost/host.go#L633) — blob `2f9102997bb0d124fc3db550a1fbf3f7bd004152`; source inspected.
- [internal/runner/runner.go:619–673](../../../../internal/runner/runner.go#L619) — blob `e192af27a26f9e8b264721e665bf254aea683d2d`; source inspected.
- [internal/runner/lifecycle.go:65–103](../../../../internal/runner/lifecycle.go#L65) — blob `e48614233810276802d4dd5b46aa18d9f0d17784`; source inspected.
- [internal/runner/cooperative_stop.go:19–49](../../../../internal/runner/cooperative_stop.go#L19) — blob `fabb5d8a2ade114e57cc1931ee448aa9fba5d78f`; source inspected.
- [internal/runner/cooperative_stop_test.go:77–237](../../../../internal/runner/cooperative_stop_test.go#L77) — blob `b290c4517e17d4988e7922f626edacbbac05f388`; test source inspected; not executed.
- [docs/work-reference.md:132–155](../../../../docs/work-reference.md#L132) — blob `29be083d22dc1609f1d310177191d4112541ee17`; source inspected.

## C16

Notification methods, strict LF-inclusive wire/shape checks and unresolved response correlation.

- [internal/sessionhost/notification.go:9–134](../../../../internal/sessionhost/notification.go#L9) — blob `cf668fe46d05f827b2c1d5dda6c7b9780c885be9`; source inspected.
- [internal/sessionhost/notification_transport.go:12–94](../../../../internal/sessionhost/notification_transport.go#L12) — blob `22624307d840f1a914b7bd15e5bb05ca65692352`; source inspected.
- [internal/sessionhost/notification_transport.go:104–197](../../../../internal/sessionhost/notification_transport.go#L104) — blob `22624307d840f1a914b7bd15e5bb05ca65692352`; source inspected.
- [internal/sessionhost/notification_test.go:106–147](../../../../internal/sessionhost/notification_test.go#L106) — blob `da4bcd56497b23bc1397dccfecff5b81ed0de3ee`; test source inspected; not executed.
- [internal/sessionhost/notification_test.go:306–350](../../../../internal/sessionhost/notification_test.go#L306) — blob `da4bcd56497b23bc1397dccfecff5b81ed0de3ee`; test source inspected; not executed.

## C17

Private notification journal and stream gates, same-attempt custody and terminal loss.

- [internal/sessionhost/notification_journal.go:11–19](../../../../internal/sessionhost/notification_journal.go#L11) — blob `5c872f6fd3b29159d311edc8b87a5e3468161ab1`; source inspected.
- [internal/sessionhost/notification_journal.go:55–117](../../../../internal/sessionhost/notification_journal.go#L55) — blob `5c872f6fd3b29159d311edc8b87a5e3468161ab1`; source inspected.
- [internal/sessionhost/notification_journal.go:135–270](../../../../internal/sessionhost/notification_journal.go#L135) — blob `5c872f6fd3b29159d311edc8b87a5e3468161ab1`; source inspected.
- [internal/sessionhost/notification_stream.go:15–64](../../../../internal/sessionhost/notification_stream.go#L15) — blob `11f0eb0635c2640a12e16140877fdb9f444347ee`; source inspected.
- [internal/sessionhost/notification_stream.go:66–156](../../../../internal/sessionhost/notification_stream.go#L66) — blob `11f0eb0635c2640a12e16140877fdb9f444347ee`; source inspected.
- [internal/sessionhost/notification_journal_test.go:106–140](../../../../internal/sessionhost/notification_journal_test.go#L106) — blob `71facd7e96a8d744def24bfeceb71f17dda8f79a`; test source inspected; not executed.
- [internal/sessionhost/notification_journal_test.go:187–229](../../../../internal/sessionhost/notification_journal_test.go#L187) — blob `71facd7e96a8d744def24bfeceb71f17dda8f79a`; test source inspected; not executed.
- [internal/sessionhost/notification_journal_test.go:391–415](../../../../internal/sessionhost/notification_journal_test.go#L391) — blob `71facd7e96a8d744def24bfeceb71f17dda8f79a`; test source inspected; not executed.
- [internal/sessionhost/notification_stream_test.go:93–256](../../../../internal/sessionhost/notification_stream_test.go#L93) — blob `b741bdbcd352dae6103c5324a6465a9c98133766`; test source inspected; not executed.

## C18

Private manager custody/enrollment seam, one-shot CLOEXEC handoff and local ready limits.

- [internal/sessionhost/notification_manager.go:14–46](../../../../internal/sessionhost/notification_manager.go#L14) — blob `469e95f735fcfaa05ecf6a646a6f889a5c82ec5e`; source inspected.
- [internal/sessionhost/notification_manager.go:83–151](../../../../internal/sessionhost/notification_manager.go#L83) — blob `469e95f735fcfaa05ecf6a646a6f889a5c82ec5e`; source inspected.
- [internal/sessionhost/notification_manager.go:159–239](../../../../internal/sessionhost/notification_manager.go#L159) — blob `469e95f735fcfaa05ecf6a646a6f889a5c82ec5e`; source inspected.
- [internal/sessionhost/notification_manager.go:242–304](../../../../internal/sessionhost/notification_manager.go#L242) — blob `469e95f735fcfaa05ecf6a646a6f889a5c82ec5e`; source inspected.
- [internal/sessionhost/notification_manager_test.go:94–183](../../../../internal/sessionhost/notification_manager_test.go#L94) — blob `36f8f35ff43f69076ac76de5d80505e6130870e1`; test source inspected; not executed.
- [internal/sessionhost/notification_manager_test.go:336–390](../../../../internal/sessionhost/notification_manager_test.go#L336) — blob `36f8f35ff43f69076ac76de5d80505e6130870e1`; test source inspected; not executed.
- [docs/notification-lifecycle.md:3–30](../../../../docs/notification-lifecycle.md#L3) — blob `f69677069815dc9d3ac1a9421f6141bd2f7ce0f2`; source inspected.

## C19

Compiled internal adapter seams and explicit CLI wiring; no external plugin interface.

- [internal/sessionhost/host.go:49–78](../../../../internal/sessionhost/host.go#L49) — blob `2f9102997bb0d124fc3db550a1fbf3f7bd004152`; source inspected.
- [internal/sessionhost/host.go:111–127](../../../../internal/sessionhost/host.go#L111) — blob `2f9102997bb0d124fc3db550a1fbf3f7bd004152`; source inspected.
- [internal/sessionhost/host.go:180–192](../../../../internal/sessionhost/host.go#L180) — blob `2f9102997bb0d124fc3db550a1fbf3f7bd004152`; source inspected.
- [internal/sessionhost/host.go:231–260](../../../../internal/sessionhost/host.go#L231) — blob `2f9102997bb0d124fc3db550a1fbf3f7bd004152`; source inspected.
- [internal/sessionhost/host.go:495–531](../../../../internal/sessionhost/host.go#L495) — blob `2f9102997bb0d124fc3db550a1fbf3f7bd004152`; source inspected.
- [internal/fixtureadapters/adapters.go:15–85](../../../../internal/fixtureadapters/adapters.go#L15) — blob `8326784a138340989e5ef0a47c8a37e302778c96`; source inspected.
- [internal/adapterprotocol/contract.go:29–77](../../../../internal/adapterprotocol/contract.go#L29) — blob `521829cbf54cbf2d668f4630cbe12a215c1a94dd`; source inspected.
- [internal/adapterprotocol/contract.go:108–187](../../../../internal/adapterprotocol/contract.go#L108) — blob `521829cbf54cbf2d668f4630cbe12a215c1a94dd`; source inspected.
- [internal/adapterprotocol/contract.go:204–274](../../../../internal/adapterprotocol/contract.go#L204) — blob `521829cbf54cbf2d668f4630cbe12a215c1a94dd`; source inspected.
- [cmd/swarma-session-host/main.go:68–85](../../../../cmd/swarma-session-host/main.go#L68) — blob `819e36a5fa1e402959dbc81ecc040a1c754eb40f`; source inspected.
- [internal/adapterprotocol/contract_test.go:15–144](../../../../internal/adapterprotocol/contract_test.go#L15) — blob `72e93f4cf909d628df12e3aea3cabf61e1204679`; test source inspected; not executed.
- [internal/adapterprotocol/capabilities_test.go:8–140](../../../../internal/adapterprotocol/capabilities_test.go#L8) — blob `040ca3266b4ef4df20ccca3da06fffe0472dfbd7`; test source inspected; not executed.

## C20

Metadata custody limits; closed state is not general secret classification.

- [internal/sessionhost/host.go:43–47](../../../../internal/sessionhost/host.go#L43) — blob `2f9102997bb0d124fc3db550a1fbf3f7bd004152`; source inspected.
- [internal/sessionhost/host_test.go:302–336](../../../../internal/sessionhost/host_test.go#L302) — blob `504057ea31015088254d4142c108c42bb3f10fb4`; test source inspected; not executed.
- [docs/prototype-m1.md:48–55](../../../../docs/prototype-m1.md#L48) — blob `67f47adbe1f83912526eb370d56325eb1218ea99`; source inspected.
- [internal/sessionhost/protocol.go:18–39](../../../../internal/sessionhost/protocol.go#L18) — blob `953966db988961d33f8210f04d66372ddad28215`; source inspected.
- [docs/prototype-m2.md:207–211](../../../../docs/prototype-m2.md#L207) — blob `10999233cd595b8e32059933a745bfc173367b9a`; source inspected.

## C21

Read-only injected synthetic admission, exact tuple/revision checks and durable loss fence.

- [internal/admission/admission.go:1–3](../../../../internal/admission/admission.go#L1) — blob `d1f784c26b74bcb56ea49b3df7aa7a40a6f607bc`; source inspected.
- [internal/admission/admission.go:22–92](../../../../internal/admission/admission.go#L22) — blob `d1f784c26b74bcb56ea49b3df7aa7a40a6f607bc`; source inspected.
- [internal/admission/admission.go:101–173](../../../../internal/admission/admission.go#L101) — blob `d1f784c26b74bcb56ea49b3df7aa7a40a6f607bc`; source inspected.
- [internal/sessionhost/host.go:193–199](../../../../internal/sessionhost/host.go#L193) — blob `2f9102997bb0d124fc3db550a1fbf3f7bd004152`; source inspected.
- [internal/sessionhost/admission.go:10–61](../../../../internal/sessionhost/admission.go#L10) — blob `f729d28fed82b7eb91ea0b1a4164829df232f4a4`; source inspected.
- [internal/sessionhost/host.go:587–596](../../../../internal/sessionhost/host.go#L587) — blob `2f9102997bb0d124fc3db550a1fbf3f7bd004152`; source inspected.
- [internal/sessionhost/admission_matrix_test.go:16–142](../../../../internal/sessionhost/admission_matrix_test.go#L16) — blob `8df19053796714b648f68f489879748370d2edd6`; test source inspected; not executed.
- [internal/sessionhost/admission_test.go:476–501](../../../../internal/sessionhost/admission_test.go#L476) — blob `8775504dbd92c6aaef06c060cea835afb309d28f`; test source inspected; not executed.
- [docs/prototype-m3-synthetic.md:26–41](../../../../docs/prototype-m3-synthetic.md#L26) — blob `ebbb8b4d91093935fcc30f9cfd6712715e16d06d`; source inspected.
- [docs/prototype-m3-synthetic.md:112–123](../../../../docs/prototype-m3-synthetic.md#L112) — blob `ebbb8b4d91093935fcc30f9cfd6712715e16d06d`; source inspected.

## C22

Separate runner identify snapshot, unexported qualification client and absent live admission proof.

- [internal/runner/identify.go:10–106](../../../../internal/runner/identify.go#L10) — blob `de14bb920383ec47f7b670bf2ceb1611a1072c9e`; source inspected.
- [internal/runner/identify_client.go:47–65](../../../../internal/runner/identify_client.go#L47) — blob `a26b700f6428720b127bd3c6e4f49d3dba91a227`; source inspected.
- [internal/runner/identify_snapshot_test.go:35–77](../../../../internal/runner/identify_snapshot_test.go#L35) — blob `b752cc88bc2e00239b9ce81ab5dd91cf88278984`; test source inspected; not executed.
- [internal/runner/identify_snapshot_test.go:120–152](../../../../internal/runner/identify_snapshot_test.go#L120) — blob `b752cc88bc2e00239b9ce81ab5dd91cf88278984`; test source inspected; not executed.
- [internal/localipc/identity_linux.go:8–12](../../../../internal/localipc/identity_linux.go#L8) — blob `9d2af85dbd3a60d043de9c326d9341d9d848aa04`; source inspected.
- [docs/runner-identify.md:8–17](../../../../docs/runner-identify.md#L8) — blob `e94a9ae689408e486322436090b2cf5ae8380f67`; source inspected.
- [docs/runner-identify.md:68–74](../../../../docs/runner-identify.md#L68) — blob `e94a9ae689408e486322436090b2cf5ae8380f67`; source inspected.
- [docs/runner-identify.md:112–155](../../../../docs/runner-identify.md#L112) — blob `e94a9ae689408e486322436090b2cf5ae8380f67`; source inspected.

## C23

Opt-in inert docking helper is a test executable, not a product activation interface.

- [docs/notification-docking-fixture.md:3–34](../../../../docs/notification-docking-fixture.md#L3) — blob `467ddef6378bf951a9a48d356ea7a931781a3e5f`; source inspected.
- [docs/notification-docking-fixture.md:66–79](../../../../docs/notification-docking-fixture.md#L66) — blob `467ddef6378bf951a9a48d356ea7a931781a3e5f`; source inspected.
- [internal/sessionhost/notification_docking_test.go:51–71](../../../../internal/sessionhost/notification_docking_test.go#L51) — blob `a8104317f2e008572a264838d238b432ef6bef7d`; test source inspected; not executed.
- [internal/sessionhost/notification_docking_test.go:109–211](../../../../internal/sessionhost/notification_docking_test.go#L109) — blob `a8104317f2e008572a264838d238b432ef6bef7d`; test source inspected; not executed.
- [internal/sessionhost/notification_docking_test.go:232–245](../../../../internal/sessionhost/notification_docking_test.go#L232) — blob `a8104317f2e008572a264838d238b432ef6bef7d`; test source inspected; not executed.

## C24

Historical extraction/design material does not establish current interfaces.

- [README.md:126–138](../../../../README.md#L126) — blob `bfa6847c286317fc28db7e6229dd569623dfcaea`; source inspected.
- [docs/sh1-gate.md:3–11](../../../../docs/sh1-gate.md#L3) — blob `e11c99211f1309ebe54280235deb51defae7f8a3`; source inspected.
- [spec/session-host.md:3–11](../../../../spec/session-host.md#L3) — blob `511887fc85ffa38bf5d9d623767829018d8707ac`; source inspected.
- [diagrams/README.md:9–14](../../../../diagrams/README.md#L9) — blob `41ace490481e899867b9df823cc08a3316b40700`; source inspected.
