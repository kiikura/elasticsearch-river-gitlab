package org.devtools.river;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

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
import org.gitlab.api.models.GitlabMergeRequest;
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
	
	private int mrMax;

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
		this.issueMax = XContentMapValues.nodeIntegerValue(
				gitlabSettings.get("mr_max"), -1);
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
							updateMergeRequestIndex(project);
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

		/**
		 * MergeRequestを更新
		 * @param project
		 */
		private void updateMergeRequestIndex(GitlabProject project) {
			logger.info("START GitlabRiverLogic.updateMergeRequestIndex: " + project.getPathWithNamespace());
			String mrTail = String.format("%s/%s%s", GitlabProject.URL,project.getId(),GitlabMergeRequest.URL);
			Iterator<GitlabMergeRequest[]> issuesIterator = api.retrieve().asIterator(mrTail, GitlabMergeRequest[].class);
			int mrCounter=0;
			boolean breakFlg = false;
			while(issuesIterator.hasNext() && !breakFlg){
				GitlabMergeRequest[] mrsPage =issuesIterator.next();
				for (GitlabMergeRequest mr : mrsPage) {
					if(mrMax > 0 && mrMax < mrCounter++) {
						breakFlg = true;
						break;
					}
					List<GitlabNote> notes = getMergeRequestNotes(mr);
					
					try {
						IndexRequest req = Requests.indexRequest(indexName)
								.type("mergerequest")
								.id(String.valueOf(mr.getId()))
								.source(toMergeRequestJson(project,mr,notes));
						bulkProcessor.add(req);
					} catch (IOException e) {
						logger.warn("mergerequest index", e);
					}
				}
			}
		}
		
		

		private XContentBuilder toMergeRequestJson(GitlabProject project,
				GitlabMergeRequest mr, List<GitlabNote> notes) throws IOException {
			
			
			XContentBuilder builder = XContentFactory.jsonBuilder();
			builder.startObject();//issue root start
			builder.field("id",mr.getId())
				   .field("project_id",mr.getProjectId())
				   .field("project_path",project.getPathWithNamespace())
				   .field("iid",mr.getIid())
				   .field("title",mr.getTitle())
				   .field("description",mr.getDescription())
				   .field("state",mr.getState())
				   .field("source_project_id", mr.getSourceProjectId())
				   .field("source_branch", mr.getSourceBranch())
				   .field("target_branch", mr.getTargetBranch())
				   .field("milestone_id", mr.getMilestoneId())
				   .field("author",mr.getAuthor().getUsername());
			if(mr.getAssignee() != null)
				builder.field("assignee",mr.getAssignee().getUsername());
			
			//add notes
			builder.startArray("notes");
			appendNotesArray(notes, builder);
			builder.endArray();
			
			//issue root end
			builder.endObject();
			return builder;
		}

		/**
		 * Issueを更新
		 * @param project
		 */
		private void updateIssueIndex(GitlabProject project) {
			logger.info("START GitlabRiverLogic.updateIssueIndex: " + project.getPathWithNamespace());
			String issuesTail = String.format("%s/%s%s", GitlabProject.URL,project.getId(),GitlabIssue.URL);
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
				   .field("updated_at",issue.getUpdatedAt());
			if(issue.getAssignee() != null)
				builder.field("assignee",issue.getAssignee().getUsername());
			if(issue.getMilestone() != null){
				builder.field("milestone",issue.getMilestone().getTitle());
				builder.field("milestone_id",issue.getMilestone().getId());
			}
			builder.array("labels",issue.getLabels());
			
			//add notes
			builder.startArray("notes");
			appendNotesArray(notes, builder);
			builder.endArray();
			
			//issue root end
			builder.endObject();
			return builder;
		}

		private void appendNotesArray(List<GitlabNote> notes,
				XContentBuilder builder) throws IOException {
			for(GitlabNote note : notes){
				builder.startObject();
				builder.field("body", note.getBody());
				builder.field("id", note.getId());
				builder.field("create_at", note.getCreatedAt());
				builder.field("attachment", note.getAttachment());
				builder.field("author", note.getAuthor().getUsername());
				builder.endObject();
			}
		}

		private List<GitlabNote> getIssueNotes(GitlabIssue issue) {
			try {
				return api.getNotes(issue);
			} catch (IOException e) {
				logger.error(
						"fail getIssueNotes() issue.projectId={} issue.iid={}",
						e, issue.getProjectId(), issue.getIid());
				return Collections.EMPTY_LIST;
			}
		}
		
		private List<GitlabNote> getMergeRequestNotes(GitlabMergeRequest mr) {
			try {
				return api.getNotes(mr);
			} catch (IOException e) {
				logger.error(
						"fail getMergeRequestNotes() mr.projectId={} mr.iid={}",
						e, mr.getProjectId(), mr.getIid());
				return Collections.EMPTY_LIST;
			}
		}
	}
}
