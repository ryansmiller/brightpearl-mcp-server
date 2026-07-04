"""Config for dynamically synced resources (probed against the live API 2026-07).

Three kinds:
- SEARCH_DUMPS: resource-search results loaded as typed columns (schema comes
  from the search's own metaData). incremental modes:
    ("id", col)      — append-only resources, cursor on a monotonically
                       increasing id column
    ("updated", col) — resources with a filterable updatedOn column
    ("full", None)   — small or mutable-without-updatedOn resources,
                       truncate-and-reload every run
- REFERENCE_GETS: plain GET collections, truncate-and-reload (all tiny)
- Derived loaders (product prices/availability) live in sync/derived.py
"""

SEARCH_DUMPS: dict[str, dict] = {
    # 1.43M rows: one row per journal line (debit/credit per nominal code),
    # so this covers SyncHub's Journal + JournalCredit + JournalDebit
    "journal_rows": {
        "search": ("accounting-service", "journal"),
        "key": "journalRowId",
        "incremental": ("id", "journalRowId"),
        "tier": "warm",
    },
    "customer_payments": {
        "search": ("accounting-service", "customer-payment"),
        "key": "paymentId",
        "incremental": ("id", "paymentId"),
        "tier": "warm",
    },
    "supplier_payments": {
        "search": ("accounting-service", "supplier-payment"),
        "key": "paymentId",
        "incremental": ("id", "paymentId"),
        "tier": "warm",
    },
    "goods_movements": {
        "search": ("warehouse-service", "goods-movement"),
        "key": "goodsMovementId",
        "incremental": ("updated", "updatedOn"),
        "tier": "hot",
    },
    # picked/packed/shipped flags mutate and there is no updatedOn column →
    # full reload (~270 requests/run)
    "goods_out_notes": {
        "search": ("warehouse-service", "goods-note/goods-out"),
        "key": "goodsOutNoteId",
        "incremental": ("full", None),
        "tier": "warm",
    },
    "companies": {
        "search": ("contact-service", "company"),
        "key": "companyId",
        "incremental": ("full", None),
        "tier": "warm",
    },
    "brands": {
        "search": ("product-service", "brand"),
        "key": "brandId",
        "incremental": ("full", None),
        "tier": "cold",
    },
    "collections": {
        "search": ("product-service", "collection"),
        "key": "collectionId",
        "incremental": ("full", None),
        "tier": "cold",
    },
    "product_types": {
        "search": ("product-service", "product-type"),
        "key": "id",
        "incremental": ("full", None),
        "tier": "cold",
    },
    "seasons": {
        "search": ("product-service", "season"),
        "key": "id",
        "incremental": ("full", None),
        "tier": "cold",
    },
    "contact_groups": {
        "search": ("contact-service", "contact-group"),
        "key": "id",
        "incremental": ("full", None),
        "tier": "cold",
    },
    "nominal_codes": {
        "search": ("accounting-service", "nominal-code"),
        "key": "code",
        "incremental": ("full", None),
        "tier": "cold",
    },
    "currencies": {
        "search": ("accounting-service", "currency"),
        "key": "id",
        "incremental": ("full", None),
        "tier": "cold",
    },
    "payment_methods": {
        "search": ("accounting-service", "payment-method"),
        "key": "id",
        "incremental": ("full", None),
        "tier": "cold",
    },
    "warehouses": {
        "search": ("warehouse-service", "warehouse"),
        "key": "id",
        "incremental": ("full", None),
        "tier": "cold",
    },
    "shipping_methods": {
        "search": ("warehouse-service", "shipping-method"),
        "key": "id",
        "incremental": ("full", None),
        "tier": "cold",
    },
    "categories": {
        "search": ("product-service", "brightpearl-category"),
        "key": "id",
        "incremental": ("full", None),
        "tier": "cold",
    },
    "warehouse_locations": {
        "search": ("warehouse-service", "location"),
        "key": "id",
        "incremental": ("full", None),
        "tier": "cold",
    },
    "product_options": {
        "search": ("product-service", "option"),
        "key": "id",
        "incremental": ("full", None),
        "tier": "cold",
    },
    "product_option_values": {
        "search": ("product-service", "option-value"),
        "key": "id",
        "incremental": ("full", None),
        "tier": "cold",
    },
    "contact_group_members": {
        "search": ("contact-service", "contact-group-member"),
        "key": "id",
        "incremental": ("full", None),
        "tier": "cold",
    },
}

# Plain GET collection endpoints → truncate-and-reload reference tables
REFERENCE_GETS: dict[str, str] = {
    "order_statuses": "order-service/order-status",
    "order_types": "order-service/order-type",
    "order_stock_statuses": "order-service/order-stock-status",
    "order_shipping_statuses": "order-service/order-shipping-status",
    "tax_codes": "accounting-service/tax-code",
    "accounting_periods": "accounting-service/accounting-period",
    "lead_sources": "contact-service/lead-source",
    "contact_tags": "contact-service/tag",
    "price_lists": "product-service/price-list",
    "channel_brands": "product-service/channel-brand",
    "channels": "product-service/channel",
}
