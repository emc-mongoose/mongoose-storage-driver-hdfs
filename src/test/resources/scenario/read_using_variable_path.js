new java.lang.ProcessBuilder()
	.command("sh", "-c", "rm -f " + ITEM_LIST_FILE)
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
					"file": ITEM_LIST_FILE,
					"path": ITEM_OUTPUT_PATH + "/${path:random(16, 2)}"
				}
			},
			"load": {
				"op": {
					"limit": {
						"count": STEP_LIMIT_COUNT
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

ReadVerifyLoad
	.config(
		{
			"item": {
				"input": {
					"file": ITEM_LIST_FILE
				}
			}
		}
	)
	.run();
