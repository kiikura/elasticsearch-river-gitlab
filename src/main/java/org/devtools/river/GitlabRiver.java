package org.devtools.river;

import java.util.Map;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

public class GitlabRiver extends AbstractRiverComponent implements River {
	private final Client client;

	private GitlabRiverLogic riverLogic;

	private volatile Thread riverThread;

	private volatile boolean closed;

	private volatile BulkProcessor bulkProcessor;

	private String indexName;

	private String typeName;

	private int bulkSize;

	private TimeValue bulkFlushInterval;

	private int maxConcurrentBulk;

	@Inject
	public GitlabRiver(final RiverName riverName, final RiverSettings settings,
			final Client client) {
		super(riverName, settings);
		this.client = client;

		logger.info("CREATE GitlabRiver");

		Map<String, Object> gitlabSettings = (Map<String, Object>) this.settings
				.settings().get("gitlab");
		
        Map<String,Object> indexSettings = (Map<String,Object>)this.settings.settings().get("index");
        this.indexName = XContentMapValues.nodeStringValue(indexSettings.get("index"), riverName.name());
        this.typeName = XContentMapValues.nodeStringValue(indexSettings.get("type"), "page");
        this.bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
        this.bulkFlushInterval = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(
                indexSettings.get("flush_interval"), "5s"), TimeValue.timeValueSeconds(5));
        this.maxConcurrentBulk = XContentMapValues.nodeIntegerValue(indexSettings.get("max_concurrent_bulk"), 1);

	}

	@Override
	public void start() {
		logger.info("START GitlabRiver");

		// index作成

		// マッピング登録

		// バルクプロセッサ
		// Creating bulk processor
		this.bulkProcessor = BulkProcessor
				.builder(client, new BulkProcessor.Listener() {
					@Override
					public void beforeBulk(long executionId, BulkRequest request) {
						logger.debug(
								"Going to execute new bulk composed of {} actions",
								request.numberOfActions());
					}

					@Override
					public void afterBulk(long executionId,
							BulkRequest request, BulkResponse response) {
						logger.debug("Executed bulk composed of {} actions",
								request.numberOfActions());
						if (response.hasFailures()) {
							logger.warn(
									"There was failures while executing bulk",
									response.buildFailureMessage());
							if (logger.isDebugEnabled()) {
								for (BulkItemResponse item : response
										.getItems()) {
									if (item.isFailed()) {
										logger.debug(
												"Error for {}/{}/{} for {} operation: {}",
												item.getIndex(),
												item.getType(), item.getId(),
												item.getOpType(),
												item.getFailureMessage());
									}
								}
							}
						}
					}

					@Override
					public void afterBulk(long executionId,
							BulkRequest request, Throwable failure) {
						logger.warn("Error executing bulk", failure);
					}
				}).setBulkActions(bulkSize)
				.setConcurrentRequests(maxConcurrentBulk)
				.setFlushInterval(bulkFlushInterval).build();

		riverLogic = new GitlabRiverLogic();
		this.riverThread = EsExecutors.daemonThreadFactory(
				settings.globalSettings(),
				"river(" + riverName().getType() + "/" + riverName().getName()
						+ ")").newThread(riverLogic);
		this.riverThread.start();
	}

	@Override
	public void close() {
		logger.info("CLOSE GitlabRiver");
		this.closed = true;
		
		bulkProcessor.close();
		this.riverThread.interrupt();
	}

	private class GitlabRiverLogic implements Runnable {

		@Override
		public void run() {
			logger.info("START GitlabRiverLogic: " + client.toString());

			while (!closed) {

				try {
					Thread.sleep(2000L);
				} catch (InterruptedException e) {
					
				}
			}

		}
	}
}
