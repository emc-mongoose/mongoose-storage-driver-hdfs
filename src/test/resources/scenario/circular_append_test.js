Command
	.value("rm -f CircularAppendTest0.csv CircularAppendTest1.csv")
	.run();

PreconditionLoad
	.config(
		{
			"item": {
				"output": {
					"file": "CircularAppendTest0.csv"
				}
			},
			"test": {
				"step": {
					"limit": {
						"count": 100
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
						"fixed": "-${ITEM_DATA_SIZE}-"
					}
				},
				"input": {
					"file": "CircularAppendTest0.csv"
				},
				"output": {
					"file": "CircularAppendTest1.csv"
				}
			},
			"load": {
				"type": "update",
				"generator": {
					"recycle": {
						"enabled": true
					}
				}
			},
			"test": {
				"step": {
					"limit": {
						"count": 10000 // append each of 100 data items approx 100 times
					}
				}
			}
		}
	)
	.run();
