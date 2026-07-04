"""Brightpearl API payloads → BigQuery row dicts.

Money and quantity values arrive from the API as strings ("1745.50"); they are
converted to float here. Timestamps stay as ISO-8601 strings with offsets,
which BigQuery load jobs parse into TIMESTAMP natively. The complete payload is
preserved in raw_payload (JSON) so no field is ever lost.
"""

from datetime import datetime, timezone
from typing import Any


def _num(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def order_to_rows(order: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """One order payload → (orders row, order_rows rows)."""
    now = _now()
    customer = (order.get("parties") or {}).get("customer") or {}
    delivery = (order.get("parties") or {}).get("delivery") or {}
    assignment = ((order.get("assignment") or {}).get("current")) or {}
    currency = order.get("currency") or {}
    total = order.get("totalValue") or {}
    invoices = order.get("invoices") or []

    order_row = {
        "order_id": order["id"],
        "parent_order_id": order.get("parentOrderId") or None,
        "order_type_code": order.get("orderTypeCode"),
        "reference": order.get("reference"),
        "order_status_id": (order.get("orderStatus") or {}).get("orderStatusId"),
        "order_status_name": (order.get("orderStatus") or {}).get("name"),
        "order_payment_status": order.get("orderPaymentStatus"),
        "stock_status_code": order.get("stockStatusCode"),
        "allocation_status_code": order.get("allocationStatusCode"),
        "shipping_status_code": order.get("shippingStatusCode"),
        "placed_on": order.get("placedOn"),
        "created_on": order.get("createdOn"),
        "updated_on": order.get("updatedOn"),
        "closed_on": order.get("closedOn"),
        "customer_contact_id": customer.get("contactId"),
        "customer_company_name": customer.get("companyName"),
        "customer_email": customer.get("email"),
        "delivery_state": delivery.get("addressLine4"),
        "delivery_postal_code": delivery.get("postalCode"),
        "delivery_country_iso": delivery.get("countryIsoCode"),
        "warehouse_id": order.get("warehouseId"),
        "channel_id": assignment.get("channelId"),
        "lead_source_id": assignment.get("leadSourceId"),
        "staff_owner_contact_id": assignment.get("staffOwnerContactId"),
        "currency_code": currency.get("orderCurrencyCode"),
        "exchange_rate": _num(currency.get("exchangeRate")),
        "total_net": _num(total.get("net")),
        "total_tax": _num(total.get("taxAmount")),
        "total": _num(total.get("total")),
        "base_total_net": _num(total.get("baseNet")),
        "base_total": _num(total.get("baseTotal")),
        "invoice_reference": invoices[0].get("invoiceReference") if invoices else None,
        "price_list_id": order.get("priceListId"),
        "historical_order": order.get("historicalOrder"),
        "is_deleted": False,
        "deleted_at": None,
        "raw_payload": order,
        "when_upserted": now,
    }

    line_rows = []
    for row_id, row in (order.get("orderRows") or {}).items():
        value = row.get("rowValue") or {}
        composition = row.get("composition") or {}
        options = row.get("productOptions")
        line_rows.append(
            {
                "order_row_id": int(row_id),
                "order_id": order["id"],
                "row_sequence": int(row["orderRowSequence"])
                if row.get("orderRowSequence")
                else None,
                "product_id": row.get("productId"),
                "product_name": row.get("productName"),
                "product_sku": row.get("productSku"),
                "quantity": _num((row.get("quantity") or {}).get("magnitude")),
                "item_cost": _num((row.get("itemCost") or {}).get("value")),
                "product_price": _num((row.get("productPrice") or {}).get("value")),
                "discount_percentage": _num(row.get("discountPercentage")),
                "row_net": _num((value.get("rowNet") or {}).get("value")),
                "row_tax": _num((value.get("rowTax") or {}).get("value")),
                "tax_rate": _num(value.get("taxRate")),
                "tax_code": value.get("taxCode"),
                "nominal_code": row.get("nominalCode"),
                "bundle_parent": composition.get("bundleParent"),
                "bundle_child": composition.get("bundleChild"),
                "parent_order_row_id": composition.get("parentOrderRowId") or None,
                "product_options": options or None,
                "order_updated_on": order.get("updatedOn"),
                "when_upserted": now,
            }
        )
    return order_row, line_rows


def product_to_row(product: dict[str, Any]) -> dict[str, Any]:
    identity = product.get("identity") or {}
    stock = product.get("stock") or {}
    channels = product.get("salesChannels") or []
    return {
        "product_id": product["id"],
        "sku": identity.get("sku"),
        "barcode": identity.get("barcode"),
        "name": channels[0].get("productName") if channels else None,
        "status": product.get("status"),
        "brand_id": product.get("brandId"),
        "product_type_id": product.get("productTypeId"),
        "product_group_id": product.get("productGroupId"),
        "stock_tracked": stock.get("stockTracked"),
        "weight": _num((stock.get("weight") or {}).get("magnitude")),
        "is_bundle": (product.get("composition") or {}).get("bundle"),
        "nominal_code_sales": product.get("nominalCodeSales"),
        "nominal_code_purchases": product.get("nominalCodePurchases"),
        "nominal_code_stock": product.get("nominalCodeStock"),
        "created_on": product.get("createdOn"),
        "updated_on": product.get("updatedOn"),
        "is_deleted": False,
        "deleted_at": None,
        "raw_payload": product,
        "when_upserted": _now(),
    }


def contact_to_row(contact: dict[str, Any]) -> dict[str, Any]:
    comm = contact.get("communication") or {}
    emails = comm.get("emails") or {}
    phones = comm.get("telephones") or {}
    relationship = contact.get("relationshipToAccount") or {}
    financial = contact.get("financialDetails") or {}
    return {
        "contact_id": contact["contactId"],
        "first_name": contact.get("firstName"),
        "last_name": contact.get("lastName"),
        "email": (emails.get("PRI") or {}).get("email"),
        "telephone": phones.get("PRI"),
        "company_id": contact.get("companyId"),
        "organisation_name": (contact.get("organisation") or {}).get("name"),
        "is_supplier": relationship.get("isSupplier"),
        "is_staff": relationship.get("isStaff"),
        "price_list_id": financial.get("priceListId"),
        "credit_term_days": financial.get("creditTermDays"),
        "currency_id": financial.get("currencyId"),
        "contact_tags": contact.get("contactTags"),
        "created_on": contact.get("createdOn"),
        "updated_on": contact.get("updatedOn"),
        "last_contacted_on": contact.get("lastContactedOn"),
        "is_deleted": False,
        "deleted_at": None,
        "raw_payload": contact,
        "when_upserted": _now(),
    }
