package org.devtools.river;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.gitlab.api.GitlabAPI;
import org.gitlab.api.models.GitlabIssue;
import org.gitlab.api.models.GitlabNote;
import org.gitlab.api.models.GitlabProject;

public class GitlabRiver extends AbstractRiverComponent implements River {
	private final Client client;

	private GitlabRiverLogic riverLogic;

	private volatile Thread riverThread;

	private volatile boolean closed;

	private volatile BulkProcessor bulkProcessor;

	private String indexName;

	// private String typeName;

	private int bulkSize;

	private TimeValue bulkFlushInterval;

	private int maxConcurrentBulk;

	private Pattern projectRegex;

	private int issueMax;

	private int mergeRequestMax;

	private String username;

	private String privateToken;

	private String hostUrl;

	private boolean ignoreCertificateErrors;

	private TimeValue fetchInterval;

	@Inject
	public GitlabRiver(final RiverName riverName, final RiverSettings settings,
			final Client client) {
		super(riverName, settings);
		this.client = client;

		logger.info("CREATE GitlabRiver");

		Map<String, Object> gitlabSettings = (Map<String, Object>) this.settings
				.settings().get("gitlab");

		this.projectRegex = Pattern.compile(XContentMapValues.nodeStringValue(
				gitlabSettings.get("project"), riverName.name()));
		this.issueMax = XContentMapValues.nodeIntegerValue(
				gitlabSettings.get("issue_max"), -1);
		this.mergeRequestMax = XContentMapValues.nodeIntegerValue(
				gitlabSettings.get("merge_request_max"), -1);
		this.username = XContentMapValues.nodeStringValue(
				gitlabSettings.get("username"), null);
		this.privateToken = XContentMapValues.nodeStringValue(
				gitlabSettings.get("privatetoken"), null);
		this.hostUrl = XContentMapValues.nodeStringValue(
				gitlabSettings.get("host_url"), null);
		this.ignoreCertificateErrors = XContentMapValues.nodeBooleanValue(
				gitlabSettings.get("ignore_certificate_errors"), false);
		this.fetchInterval = TimeValue.parseTimeValue(
				XContentMapValues.nodeStringValue(
						gitlabSettings.get("fetch_interval"), "120m"),
						TimeValue.timeValueMinutes(120));

		Map<String, Object> indexSettings = (Map<String, Object>) this.settings
				.settings().get("index");
		this.indexName = XContentMapValues.nodeStringValue(
				indexSettings.get("index"), riverName.name());
		// this.typeName =
		// XContentMapValues.nodeStringValue(indexSettings.get("type"),
		// "issue");
		this.bulkSize = XContentMapValues.nodeIntegerValue(
				indexSettings.get("bulk_size"), 100);
		this.bulkFlushInterval = TimeValue.parseTimeValue(
				XContentMapValues.nodeStringValue(
						indexSettings.get("flush_interval"), "5s"),
				TimeValue.timeValueSeconds(5));
		this.maxConcurrentBulk = XContentMapValues.nodeIntegerValue(
				indexSettings.get("max_concurrent_bulk"), 1);

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
		private GitlabAPI api;

		private GitlabRiverLogic() {
			api = GitlabAPI.connect(hostUrl, privateToken);
			api.ignoreCertificateErrors(ignoreCertificateErrors);
		}

		@Override
		public void run() {
			logger.info("START GitlabRiverLogic: " + client.toString());
			int loops = 0;
			while (!closed) {
				logger.info("LOOPSTART GitlabRiverLogic: loop=" + (loops++));
				int projectCount=0;
				try {
					List<GitlabProject> projects = api.getProjects();
					for (GitlabProject project : projects) {
						if (projectRegex.matcher(project.getPathWithNamespace())
								.matches()) {
							updateIssueIndex(project);
							projectCount++;
						}
					}
				} catch (Exception e) {
					logger.warn("GitlabRiverLogic main loop", e);
				}

				try {
					logger.info("LOOPEND GitlabRiverLogic: projectCount=" + projectCount + " sleeping");
					Thread.sleep(fetchInterval.millis());
				} catch (InterruptedException e) {

				}
			}
			logger.info("END GitlabRiverLogic: " + client.toString());
		}

		private void updateIssueIndex(GitlabProject project) {
			logger.info("START GitlabRiverLogic.updateIssueIndex: " + project.getPathWithNamespace());
			String issuesTail = String.format("%s%s%s", GitlabProject.URL,project.getId(),GitlabIssue.URL);
			Iterator<GitlabIssue[]> issuesIterator = api.retrieve().asIterator(issuesTail, GitlabIssue[].class);
			int issueCounter=0;
			boolean breakFlg = false;
			while(issuesIterator.hasNext() && !breakFlg){
				GitlabIssue[] issuesPage =issuesIterator.next();
				for (GitlabIssue issue : issuesPage) {
					if(issueMax > 0 && issueMax < issueCounter++) {
						breakFlg = true;
						break;
					}
					List<GitlabNote> notes = getIssueNotes(issue);
					
					try {
						IndexRequest req = Requests.indexRequest(indexName)
								.type("issue")
								.id(String.valueOf(issue.getId()))
								.source(toIssueJson(project,issue,notes));
						bulkProcessor.add(req);
					} catch (IOException e) {
						logger.warn("issue index", e);
					}
				}
			}
			
		}

		private XContentBuilder toIssueJson(GitlabProject project, GitlabIssue issue,
				List<GitlabNote> notes) throws IOException {
			XContentBuilder builder = XContentFactory.jsonBuilder();
			builder.startObject();//issue root start
			builder.field("id",issue.getId())
					.field("project_id",issue.getProjectId())
				.field("project_path",project.getPathWithNamespace())
				.field("iid",issue.getIid())
				.field("title",issue.getTitle())
				.field("description",issue.getDescription())
				.field("state",issue.getState())
				.field("author",issue.getAuthor().getUsername())
				.field("created_at",issue.getCreatedAt())
				.field("updated_at",issue.getUpdatedAt())
				.field("assignee",issue.getAssignee().getUsername())
				.field("milestone",issue.getMilestone().getTitle())
				.array("labels",issue.getLabels());
			
			//add notes
//			builder.startArray("notes");
//			for(GitlabNote note : notes){
//				
//			}
//			builder.endArray();
			ObjectMapper mapper = new ObjectMapper();
			ObjectWriter writer = mapper.writer();							
			builder.rawField("notes", writer.writeValueAsBytes(notes));
			
			//issue root end
			builder.endObject();
			return builder;
		}

		private List<GitlabNote> getIssueNotes(GitlabIssue issue) {
			try {
				return api.getNotes(issue);
			} catch (IOException e) {
				logger.error(
						"faile getIssueNotes() issue.projectId={} issue.iid={}",
						e, issue.getProjectId(), issue.getIid());
				return Collections.EMPTY_LIST;
			}
		}
	}
}
