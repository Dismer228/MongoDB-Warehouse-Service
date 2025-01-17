import pymongo
from flask import Flask, request, jsonify
from bson.objectid import ObjectId

def create_app():
    app = Flask(__name__)
    mongoClient = pymongo.MongoClient("localhost", 27017)

    db = mongoClient["warehouse"]  # Select or create a database
    inventory_db = db["Products"]  # Product collection
    warehouses = db["Warehouses"]  # Warehouses collection

    @app.route('/product', methods=['PUT'])
    def register_product():
        data = request.json
        required_fields = ["id", "name", "category", "price"]
        
        # Data type validation
        if not all(field in data for field in required_fields):
            return "Invalid input, missing name or price", 400
        if not isinstance(data["id"], str):
            return "Invalid input, 'id' must be a string", 400
        if not isinstance(data["name"], str):
            return "Invalid input, 'name' must be a string", 400
        if not isinstance(data["category"], str):
            return "Invalid input, 'category' must be a string", 400
        if not isinstance(data["price"], (int, float)):
            return "Invalid input, 'price' must be a number", 400

        # Checking whether such a product exists
        existing_product = inventory_db.find_one({"id": data["id"]})
        if existing_product:
            return "Product with this id already exists", 400

        inventory_db.insert_one({
            "id": data["id"],
            "name": data["name"],
            "category": data["category"],
            "price": data["price"]
        })

        return {"id": data["id"]}, 201

    @app.route('/product', methods=['GET'])
    def list_products():
        data = request.json
        category = data.get("category")

        # Checking that four categories of products are specified
        if category:
            products = inventory_db.find({"category": category})
        else:
            products = inventory_db.find()

        product_list = [{"id": product["id"], "name": product["name"], "category": product["category"], "price": product["price"]} for product in products]

        return (product_list), 200

    @app.route('/product/<productId>', methods=['GET'])
    def get_product_details(productId):
        product = inventory_db.find_one({"id": productId})

        if not product:
            return "Product not found", 404
        
        return {"id": product["id"], "name": product["name"], "price": product["price"]}, 200

    @app.route('/product/<productId>', methods=['DELETE'])
    def delete_product(productId):
        result = inventory_db.delete_one({"id": productId})

        if result.deleted_count == 0:
            return "Product not found", 404
        
        return "Product deleted", 204



    @app.route('/warehouses', methods=['PUT'])
    def register_warehouse():
        data = request.json
        required_fields = ["name", "location", "capacity"]

        # Checking that all required fields are filled in
        if not all(field in data for field in required_fields):
            return "Invalid input, missing name, location or capacity", 400

        if not isinstance(data["name"], str):
            return "'name' must be string", 400
        if not isinstance(data["location"], str):
            return "'name' must be string", 400
        if not isinstance(data["capacity"], int):
            return "'name' must be integer", 400

        new_warehouse = {
            "name": data["name"],
            "location": data["location"],
            "capacity": data["capacity"],
            "inventory": []
        }

        warehouse_new = warehouses.insert_one(new_warehouse)
        warehouseId = str(warehouse_new.inserted_id) # Warehouse generated id
        return {"id": warehouseId}, 201

    @app.route('/warehouses/<warehouseId>', methods=['GET'])
    def get_warehouse_details(warehouseId):
        warehouse = warehouses.find_one({"_id": ObjectId(warehouseId)})

        if not warehouse:
            return "Warehouse not found", 404
        
        return {"id": str(warehouse["_id"]), "name": warehouse["name"], "location": warehouse["location"], "capacity": warehouse["capacity"]}, 200

    @app.route('/warehouses/<warehouseId>', methods=['DELETE'])
    def delete_warehouse_and_inventory(warehouseId):
        result = warehouses.delete_one({"_id": ObjectId(warehouseId)})

        if result.deleted_count == 0:
            return "Warehouse not found", 404
        
        return "Warehouse deleted", 204

    @app.route('/warehouses/<warehouseId>/inventory', methods=['PUT'])
    def add_product_to_inventory(warehouseId):
        data = request.json
        requested_fields = ["productId", "quantity"]

        if not all(field in data for field in requested_fields):
            return "Invalid input, missing productId or quantity", 400
        
        
        if not isinstance(data["productId"], str):
            return "'productId' must be a string", 400  
        if not isinstance(data["quantity"], int) or data["quantity"] <= 0:
            return "'quantity' must be a positive integer", 400

        # Checking whether a product exists in the "Products" collection
        product = inventory_db.find_one({"id": data["productId"]})
        if not product:
            return "Product not found in the 'Products' collection", 404

        warehouse = warehouses.find_one({"_id": ObjectId(warehouseId)})
        if not warehouse:
            return "Warehouse not found", 404

        # Checking whether the product is already in the warehouse inventory
        inventory_item = next((item for item in warehouse.get("inventory", []) if item["productId"] == data["productId"]), None)

        if inventory_item:
            # Atnaujinti kiekį, jei produktas jau yra sandėlyje
            inventory_item["quantity"] += data["quantity"]
            inventory_id = inventory_item["_id"]
        else:
            # Update the quantity if the product is already in stock
            inventory_id = ObjectId()
            warehouse["inventory"].append({
                "_id": inventory_id,
                "productId": data["productId"],
                "quantity": data["quantity"]
            })

        # Updating a warehouse document in MongoDB
        warehouses.update_one({"_id": ObjectId(warehouseId)}, {"$set": {"inventory": warehouse["inventory"]}})

        return {"id": str(inventory_id)}, 200

    @app.route('/warehouses/<warehouseId>/inventory', methods=['GET'])
    def get_warehouse_inventory(warehouseId):
        warehouse = warehouses.find_one({"_id": ObjectId(warehouseId)})
        if not warehouse:
            return "Warehouse or inventory not found", 404

        inventory = warehouse.get("inventory", [])
        if not inventory:
            return "Warehouse or inventory not found", 404

        inventory_response = [{"id": str(item["_id"]), "productId": item["productId"], "quantity": item["quantity"]} for item in inventory]
        return (inventory_response), 200

    @app.route('/warehouses/<warehouseId>/inventory/<inventoryId>', methods=['GET'])
    def get_warehouse_inventory_detail(warehouseId, inventoryId):
        warehouse = warehouses.find_one({"_id": ObjectId(warehouseId)})

        if not warehouse:
            return "Warehouse or inventory not found", 404

        inventory_item = next((item for item in warehouse.get("inventory", []) if str(item["_id"]) == inventoryId), None)
        if not inventory_item:
            return "Inventory item not found", 404

        inventory_response = {"id": str(inventory_item["_id"]), "productId": inventory_item["productId"], "quantity": inventory_item["quantity"]}
        return (inventory_response), 200
    

    @app.route('/warehouses/<warehouseId>/inventory/<inventoryId>', methods=['DELETE'])
    def delete_product_from_inventory(warehouseId, inventoryId):
        warehouse = warehouses.find_one({"_id": ObjectId(warehouseId)})

        if not warehouse:
            return "Warehouse not found", 404

    # Find a product in the warehouse inventory by inventoryId
        inventory_item = next(
            (item for item in warehouse.get("inventory", []) if str(item["_id"]) == inventoryId), 
            None
        )

        if not inventory_item:
            return "Inventory item not found", 404

    # Remove the product from inventory
        updated_inventory = [item for item in warehouse["inventory"] if str(item["_id"]) != inventoryId]

    # Updating warehouse inventory in MongoDB
        warehouses.update_one(
            {"_id": ObjectId(warehouseId)}, 
            {"$set": {"inventory": updated_inventory}}
        )

        return "Product removed from warehouse inventory", 204

    @app.route('/warehouses/<warehouseId>/value', methods=['GET'])
    def get_warehouse_total_value(warehouseId):
        warehouse = warehouses.find_one({"_id": ObjectId(warehouseId)})

        if not warehouse:
            return "Warehouse not found", 404

        pipeline = [
            {"$match": {"_id": ObjectId(warehouseId)}},
            {"$unwind": "$inventory"},
            {"$lookup": {
                "from": "Products",
                "localField": "inventory.productId",
                "foreignField": "id",
                "as": "product_info"
            }},
            {"$unwind": "$product_info"},
            {"$project": {
                "product_price": "$product_info.price",
                "product_quantity": "$inventory.quantity"
            }},
            {"$group": {
                "_id": None,
                "total_value": {"$sum": {"$multiply": ["$product_price", "$product_quantity"]}}
            }}
        ]


        result = list(warehouses.aggregate(pipeline))

        total_value = result[0]["total_value"] if result else 0

        return {"value": round(total_value, 2)}, 200

    @app.route('/statistics/warehouses/capacity', methods=['GET'])
    def get_warehouse_capacity_stats():
        pipeline = [
        # Total capacity calculation
            {
                "$group": {
                    "_id": None,
                    "totalCapacity": {"$sum": "$capacity"},
                    "usedCapacity": {"$sum": {"$sum": "$inventory.quantity"}}
                }
            },
        # Free capacity calculation 
            {
                "$project": {
                    "totalCapacity": 1,
                    "usedCapacity": 1,
                    "freeCapacity": {"$subtract": ["$totalCapacity", "$usedCapacity"]}
                }
            }
        ]

        result = list(warehouses.aggregate(pipeline))

    # Delete a result or set a default if it is empty
        if result:
            stats = result[0]
            return {
                "totalCapacity": stats["totalCapacity"],
                "usedCapacity": stats["usedCapacity"],
                "freeCapacity": stats["freeCapacity"]
            }, 200
        else:
        # If no warehouses are found, zero capacity statistics are returned
            return {
                "totalCapacity": 0,
                "usedCapacity": 0,
                "freeCapacity": 0
            }, 200

    @app.route('/statistics/products/by/category', methods=['GET'])
    def get_product_category_stats():
    # MongoDB aggregation pipeline for grouping by category and counting products
        pipeline = [
            {
                "$group": {
                    "_id": "$category",  # Grouping by category field
                    "count": { "$sum": 1 }  # Count the number of products in each category
                }
            },
            {
                "$project": {
                    "_id": 0,  # _id field not included in the result
                    "category": "$_id",  # Rename _id to category
                    "count": 1  # Add calculation field
                }
            }
        ]
    
    # Launching the product collection aggregation pipeline
        category_stats = list(inventory_db.aggregate(pipeline))

        return (category_stats), 200

    @app.route('/cleanup', methods=['POST'])
    def clear_database():
        inventory_db.drop()
        
        warehouses.drop()

        return "Cleanup completed.", 200


    return app

if __name__ == '__main__':
    app = create_app()
    app.run(debug=True)
