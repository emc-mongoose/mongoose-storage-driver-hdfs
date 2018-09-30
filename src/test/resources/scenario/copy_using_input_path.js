Load
	.config(
		{
			"item": {
				"data" : {
					"size": ITEM_DATA_SIZE
				},
				"output": {
					"path": ITEM_PATH_0
				}
			},
			"load": {
				"op": {
					"limit": {
						"count": TEST_STEP_LIMIT_COUNT
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
				"input": {
					"path": ITEM_PATH_0
				},
				"output": {
					"path": ITEM_PATH_1
				}
			}
		}
	)
	.run();
