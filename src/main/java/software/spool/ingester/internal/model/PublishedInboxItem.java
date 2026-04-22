package software.spool.ingester.internal.model;

import software.spool.core.model.event.ItemPublished;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.InboxItem;

public record PublishedInboxItem(
    ItemPublished published,
    InboxItem inboxItem
) {
    public IdempotencyKey idempotencyKey() {
        return inboxItem.idempotencyKey();
    }

    public String payload() {
        return inboxItem.payload();
    }
}