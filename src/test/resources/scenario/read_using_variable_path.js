Command
	.value("rm -f " + ITEM_LIST_FILE)
	.run();

PreconditionLoad
	.config(
		{
			"item": {
				"output": {
					"file": ITEM_LIST_FILE,
					"path": ITEM_OUTPUT_PATH + "/%p{16;2}"
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
