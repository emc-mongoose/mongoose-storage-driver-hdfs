new java.lang.ProcessBuilder()
	.command("sh", "-c", "rm -f " + ITEM_LIST_FILE_0 + " " + ITEM_LIST_FILE_1)
	.start()
	.waitFor();

Load
	.config(
		{
			"item": {
				"data" : {
					"size": ITEM_DATA_SIZE
				},
				"output": {
					"file": ITEM_LIST_FILE_0
				}
			},
			"load": {
				"op": {
					"limit": {
						"count": BASE_ITEMS_COUNT
					}
				}
			},
			"output": {
				"metrics": {
					"average": {
						"persist": false
					},
					"summary": {
						"persist": false
					},
					"trace": {
						"persist": false
					}
				}
			}
		}
	)
	.run();

Load
	.config(
		{
			"item": {
				"data": {
					"ranges": {
						"fixed": "-" + ITEM_DATA_SIZE + "-"
					}
				},
				"input": {
					"file": ITEM_LIST_FILE_0
				},
				"output": {
					"file": ITEM_LIST_FILE_1
				}
			},
			"load": {
				"op" : {
					"limit": {
						"count": ~~(BASE_ITEMS_COUNT * APPEND_COUNT)
					}
				},
				"type": "update",
				"recycle" : true
			}
		}
	)
	.run();
