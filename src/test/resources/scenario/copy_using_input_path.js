PreconditionLoad
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
			"test": {
				"step": {
					"limit": {
						"count": TEST_STEP_LIMIT_COUNT
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
