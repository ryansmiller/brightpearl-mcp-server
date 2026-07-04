from sync.transforms import contact_to_row, order_to_rows, product_to_row

# Trimmed + anonymized from a real API payload (see GAMEPLAN Phase 2)
ORDER = {
    "id": 100039,
    "parentOrderId": 0,
    "orderTypeCode": "SO",
    "reference": "ECF-10898",
    "orderStatus": {"orderStatusId": 28, "name": "Completed"},
    "orderPaymentStatus": "PAID",
    "stockStatusCode": "SOA",
    "placedOn": "2021-01-14T12:00:00.000-05:00",
    "createdOn": "2021-06-09T16:43:44.000-04:00",
    "updatedOn": "2022-01-04T20:19:07.000-05:00",
    "currency": {"orderCurrencyCode": "USD", "exchangeRate": "1.000000"},
    "totalValue": {
        "net": "1745.50", "taxAmount": "0.00", "baseNet": "1745.50",
        "baseTotal": "1745.50", "total": "1745.50",
    },
    "assignment": {"current": {"channelId": 1, "leadSourceId": 3, "staffOwnerContactId": 0}},
    "parties": {
        "customer": {"contactId": 2329, "companyName": "Test Co", "email": "t@example.com"},
        "delivery": {"addressLine4": "CT", "postalCode": "06830", "countryIsoCode": "US"},
    },
    "invoices": [{"invoiceReference": "SI-504126"}],
    "orderRows": {
        "81": {
            "orderRowSequence": "20",
            "productId": 64185,
            "productName": "End Cap",
            "productSku": "SELECT_130-1080-07",
            "quantity": {"magnitude": "2.0000"},
            "itemCost": {"currencyCode": "USD", "value": "6.0000"},
            "productPrice": {"currencyCode": "USD", "value": "8.0000"},
            "discountPercentage": "0.00",
            "rowValue": {
                "taxRate": "0.0000", "taxCode": "N", "taxClassId": 3,
                "rowNet": {"value": "16.0000"}, "rowTax": {"value": "0.00"},
            },
            "productOptions": {"Color": "Bronze"},
            "nominalCode": "4000",
            "composition": {"bundleParent": False, "bundleChild": False, "parentOrderRowId": 0},
        }
    },
    "warehouseId": 2,
    "historicalOrder": False,
}


def test_order_transform():
    head, lines = order_to_rows(ORDER)
    assert head["order_id"] == 100039
    assert head["parent_order_id"] is None  # 0 → NULL
    assert head["order_type_code"] == "SO"
    assert head["total_net"] == 1745.50
    assert head["customer_contact_id"] == 2329
    assert head["delivery_state"] == "CT"
    assert head["invoice_reference"] == "SI-504126"
    assert head["raw_payload"]["id"] == 100039

    assert len(lines) == 1
    line = lines[0]
    assert line["order_row_id"] == 81
    assert line["order_id"] == 100039
    assert line["quantity"] == 2.0
    assert line["row_net"] == 16.0
    assert line["product_sku"] == "SELECT_130-1080-07"
    assert line["product_options"] == {"Color": "Bronze"}


def test_product_transform():
    product = {
        "id": 1000,
        "brandId": 74,
        "productTypeId": 1,
        "identity": {"sku": "ABC-1", "barcode": ""},
        "stock": {"stockTracked": True, "weight": {"magnitude": 1.5}},
        "salesChannels": [{"productName": "Test Product"}],
        "composition": {"bundle": False},
        "nominalCodeSales": "4000",
        "status": "LIVE",
        "createdOn": "2007-05-29T06:42:08.000-04:00",
        "updatedOn": "2007-09-08T10:42:45.000-04:00",
    }
    row = product_to_row(product)
    assert row["product_id"] == 1000
    assert row["sku"] == "ABC-1"
    assert row["name"] == "Test Product"
    assert row["stock_tracked"] is True
    assert row["weight"] == 1.5


def test_contact_transform():
    contact = {
        "contactId": 4,
        "firstName": "Pat",
        "lastName": "Tester",
        "communication": {
            "emails": {"PRI": {"email": "pat@example.com"}},
            "telephones": {"PRI": "555-0100"},
        },
        "relationshipToAccount": {"isSupplier": False, "isStaff": True},
        "financialDetails": {"priceListId": 1, "creditTermDays": 30, "currencyId": 1},
        "organisation": {"organisationId": 4, "name": "testco"},
        "contactTags": "0,12",
        "createdOn": "2009-02-17T10:41:29.000-05:00",
        "updatedOn": "2026-04-23T15:49:22.000-04:00",
        "companyId": 25,
    }
    row = contact_to_row(contact)
    assert row["contact_id"] == 4
    assert row["email"] == "pat@example.com"
    assert row["is_staff"] is True
    assert row["credit_term_days"] == 30
