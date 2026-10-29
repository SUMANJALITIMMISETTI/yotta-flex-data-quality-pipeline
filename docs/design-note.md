# Design and trade-offs

The implementation favors a dependency-free, inspectable batch pipeline over an external database or cloud service. CSV inputs make the source contract easy to review. The pipeline keeps a transaction event as the target grain, retains event time for late-arriving data, and adds source lineage rather than overwriting history.

Rows with data-level issues are quarantined individually so valid events can continue. Invalid customer dimension keys, unreadable files, and unexpected/missing headers fail the whole batch because reliable reference validation is then impossible. Exact duplicates retain the first input occurrence and quarantine subsequent copies; conflicting duplicate identities quarantine every copy to avoid arbitrary winner selection.

Refund checks are order-independent: the batch first indexes valid payments and sums valid refunds, then enforces the payment total. Thus file ordering does not change a decision. A payment without a refund relationship and refund events referencing a missing payment are handled explicitly. This batch contract assumes referenced payments are present in the same input batch; a production incremental pipeline would require a durable historical payment index.

Publishing uses temporary files and atomic replacement per artifact, with the report written last as a completion marker. A crash between replacements can briefly leave mixed artifacts; consumers should only read a batch when its report status is `success`. A multi-file transactional store would remove that limitation. For the requested 100,000-row scale, an in-memory implementation is simpler; streaming and chunked output plus a compact refund index are the next optimization.
