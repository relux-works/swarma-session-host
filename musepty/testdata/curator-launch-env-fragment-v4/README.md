# Curator Muse v4 cases

Copied byte for byte from `skill-agents-management v0.5.68`,
`pkg/agentic/testdata/curator-launch-env-fragment-v4/`. That corpus vendors
curator-spec `5509499`, `conformance/environments-muse-v4/schema-cases`
(CIP-0017, CIP-0015). Keep these inputs unchanged. Tests may rebind the
profile and XDG parent to isolated fixtures after loading the source case.

- `valid-muse-no-root.json`: SHA256 `3682d7d86f36ab4b170bcdc8c6bc8a27d04cbb80cda2cec59c3f45a579ff09b8`
- `valid-muse-root.json`: SHA256 `0ef56e7f171445abd12291b97f8174ef88a3f54ed1053b845538594043a564c8`
- `invalid-HOME.json`: SHA256 `d2712095a383a65a5f17b0b69296a9e01f4e9c7217664cb636de3330af5f6cd0`
- `invalid-missing-TBH_CREDENTIAL_BACKEND.json`: SHA256 `3f1ba31214a018d68de4a4db7086792fb0664cc8052cb3abbc32cfd4a79ae7e3`
- `invalid-store-native.json`: SHA256 `8badc10649a239586717be7827a1988b4396ec6d5bcc37ec316d1716e44ab609`
- `invalid-root-false.json`: SHA256 `16f35dbd714614ed894c03c41b0eda7e63ffcaab365b209a8d8a1370aa41b658`
