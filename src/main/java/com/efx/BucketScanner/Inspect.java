package com.efx.BucketScanner;

import com.google.api.core.SettableApiFuture;
import com.google.cloud.ServiceOptions;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.privacy.dlp.v2.Action;
import com.google.privacy.dlp.v2.CloudStorageOptions;
import com.google.privacy.dlp.v2.CreateDlpJobRequest;
import com.google.privacy.dlp.v2.CustomInfoType;
import com.google.privacy.dlp.v2.CustomInfoType.Dictionary;
import com.google.privacy.dlp.v2.CustomInfoType.Dictionary.WordList;
import com.google.privacy.dlp.v2.CustomInfoType.Regex;
import com.google.privacy.dlp.v2.DlpJob;
import com.google.privacy.dlp.v2.GetDlpJobRequest;
import com.google.privacy.dlp.v2.InfoType;
import com.google.privacy.dlp.v2.InfoTypeStats;
import com.google.privacy.dlp.v2.InspectConfig;
import com.google.privacy.dlp.v2.InspectConfig.FindingLimits;
import com.google.privacy.dlp.v2.InspectDataSourceDetails;
import com.google.privacy.dlp.v2.InspectJobConfig;
import com.google.privacy.dlp.v2.Likelihood;
import com.google.privacy.dlp.v2.ProjectName;
import com.google.privacy.dlp.v2.StorageConfig;
import com.google.pubsub.v1.ProjectSubscriptionName;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class Inspect {

	public static void main(String[] args) throws Exception {

		OptionGroup optionsGroup = new OptionGroup();
		optionsGroup.setRequired(true);
		Option stringOption = new Option("s", "string", true, "inspect string");
		optionsGroup.addOption(stringOption);

		Option fileOption = new Option("f", "file path", true, "inspect input file path");
		optionsGroup.addOption(fileOption);

		Option gcsOption = new Option("gcs", "Google Cloud Storage", false, "inspect GCS file");
		optionsGroup.addOption(gcsOption);

		//Option datastoreOption = new Option("ds", "Google Datastore", false, "inspect Datastore kind");
		//optionsGroup.addOption(datastoreOption);

		//Option bigqueryOption = new Option("bq", "Google BigQuery", false, "inspect BigQuery table");
		//optionsGroup.addOption(bigqueryOption);

		Options commandLineOptions = new Options();
		commandLineOptions.addOptionGroup(optionsGroup);

		Option minLikelihoodOption = Option.builder("minLikelihood").hasArg(true).required(false).build();

		commandLineOptions.addOption(minLikelihoodOption);

		Option maxFindingsOption = Option.builder("maxFindings").hasArg(true).required(false).build();

		commandLineOptions.addOption(maxFindingsOption);

		Option infoTypesOption = Option.builder("infoTypes").hasArg(true).required(false).build();
		infoTypesOption.setArgs(Option.UNLIMITED_VALUES);
		commandLineOptions.addOption(infoTypesOption);

		Option customDictionariesOption = Option.builder("customDictionaries").hasArg(true).required(false).build();
		customDictionariesOption.setArgs(Option.UNLIMITED_VALUES);
		commandLineOptions.addOption(customDictionariesOption);

		Option customRegexesOption = Option.builder("customRegexes").hasArg(true).required(false).build();
		customRegexesOption.setArgs(Option.UNLIMITED_VALUES);
		commandLineOptions.addOption(customRegexesOption);

		Option includeQuoteOption = Option.builder("includeQuote").hasArg(true).required(false).build();
		commandLineOptions.addOption(includeQuoteOption);

		Option bucketNameOption = Option.builder("bucketName").hasArg(true).required(false).build();
		commandLineOptions.addOption(bucketNameOption);

		Option gcsFileNameOption = Option.builder("fileName").hasArg(true).required(false).build();
		commandLineOptions.addOption(gcsFileNameOption);

		Option datasetIdOption = Option.builder("datasetId").hasArg(true).required(false).build();
		commandLineOptions.addOption(datasetIdOption);

		Option tableIdOption = Option.builder("tableId").hasArg(true).required(false).build();
		commandLineOptions.addOption(tableIdOption);

		Option projectIdOption = Option.builder("projectId").hasArg(true).required(false).build();
		commandLineOptions.addOption(projectIdOption);

		Option topicIdOption = Option.builder("topicId").hasArg(true).required(false).build();
		commandLineOptions.addOption(topicIdOption);

		Option subscriptionIdOption = Option.builder("subscriptionId").hasArg(true).required(false).build();
		commandLineOptions.addOption(subscriptionIdOption);

		Option datastoreNamespaceOption = Option.builder("namespace").hasArg(true).required(false).build();
		commandLineOptions.addOption(datastoreNamespaceOption);

		Option datastoreKindOption = Option.builder("kind").hasArg(true).required(false).build();
		commandLineOptions.addOption(datastoreKindOption);

		CommandLineParser parser = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd;

		try {
			cmd = parser.parse(commandLineOptions, args);
		} catch (ParseException e) {
			System.out.println(e.getMessage());
			formatter.printHelp(Inspect.class.getName(), commandLineOptions);
			System.exit(1);
			return;
		}

		Likelihood minLikelihood = Likelihood
				.valueOf(cmd.getOptionValue(minLikelihoodOption.getOpt(), Likelihood.LIKELIHOOD_UNSPECIFIED.name()));
		int maxFindings = Integer.parseInt(cmd.getOptionValue(maxFindingsOption.getOpt(), "0"));
		boolean includeQuote = Boolean.parseBoolean(cmd.getOptionValue(includeQuoteOption.getOpt(), "true"));

		String projectId = cmd.getOptionValue(projectIdOption.getOpt(), ServiceOptions.getDefaultProjectId());
		String topicId = cmd.getOptionValue(topicIdOption.getOpt());
		String subscriptionId = cmd.getOptionValue(subscriptionIdOption.getOpt());

		List<InfoType> infoTypesList = Collections.emptyList();
		if (cmd.hasOption(infoTypesOption.getOpt())) {
			infoTypesList = new ArrayList<>();
			String[] infoTypes = cmd.getOptionValues(infoTypesOption.getOpt());
			for (String infoType : infoTypes) {
				infoTypesList.add(InfoType.newBuilder().setName(infoType).build());
			}
		}

		List<CustomInfoType> customInfoTypesList = new ArrayList<>();
		if (cmd.hasOption(customDictionariesOption.getOpt())) {
			String[] dictionaryStrings = cmd.getOptionValues(customDictionariesOption.getOpt());
			for (int i = 0; i < dictionaryStrings.length; i++) {
				String[] dictionaryWords = dictionaryStrings[i].split(",");
				CustomInfoType customInfoType = CustomInfoType.newBuilder()
						.setInfoType(InfoType.newBuilder().setName(String.format("CUSTOM_DICTIONARY_%s", i)))
						.setDictionary(Dictionary.newBuilder()
								.setWordList(WordList.newBuilder().addAllWords(Arrays.<String>asList(dictionaryWords))))
						.build();
				customInfoTypesList.add(customInfoType);
			}
		}
		if (cmd.hasOption(customRegexesOption.getOpt())) {
			String[] patterns = cmd.getOptionValues(customRegexesOption.getOpt());
			for (int i = 0; i < patterns.length; i++) {
				CustomInfoType customInfoType = CustomInfoType.newBuilder()
						.setInfoType(InfoType.newBuilder().setName(String.format("CUSTOM_REGEX_%s", i)))
						.setRegex(Regex.newBuilder().setPattern(patterns[i])).build();
				customInfoTypesList.add(customInfoType);
			}
		}

		// string inspection
		if (cmd.hasOption("gcs")) {
			String bucketName = cmd.getOptionValue(bucketNameOption.getOpt());
			String fileName = cmd.getOptionValue(gcsFileNameOption.getOpt());
			inspectGcsFile(bucketName, fileName, minLikelihood, infoTypesList, customInfoTypesList, maxFindings,
					topicId, subscriptionId, projectId);
			// datastore kind inspection
		}
	}

	private static void inspectGcsFile(String bucketName, String fileName, Likelihood minLikelihood,
			List<InfoType> infoTypes, List<CustomInfoType> customInfoTypes, int maxFindings, String topicId,
			String subscriptionId, String projectId) throws Exception {
		// Instantiates a client
		try (DlpServiceClient dlpServiceClient = DlpServiceClient.create()) {

			CloudStorageOptions cloudStorageOptions = CloudStorageOptions.newBuilder()
					.setFileSet(CloudStorageOptions.FileSet.newBuilder().setUrl("gs://" + bucketName + "/" + fileName))
					.build();

			StorageConfig storageConfig = StorageConfig.newBuilder().setCloudStorageOptions(cloudStorageOptions)
					.build();

			FindingLimits findingLimits = FindingLimits.newBuilder().setMaxFindingsPerRequest(maxFindings).build();

			InspectConfig inspectConfig = InspectConfig.newBuilder().addAllInfoTypes(infoTypes)
					.addAllCustomInfoTypes(customInfoTypes).setMinLikelihood(minLikelihood).setLimits(findingLimits)
					.build();

			String pubSubTopic = String.format("projects/%s/topics/%s", projectId, topicId);
			Action.PublishToPubSub publishToPubSub = Action.PublishToPubSub.newBuilder().setTopic(pubSubTopic).build();

			Action action = Action.newBuilder().setPubSub(publishToPubSub).build();

			InspectJobConfig inspectJobConfig = InspectJobConfig.newBuilder().setStorageConfig(storageConfig)
					.setInspectConfig(inspectConfig).addActions(action).build();

			// Semi-synchronously submit an inspect job, and wait on results
			CreateDlpJobRequest createDlpJobRequest = CreateDlpJobRequest.newBuilder()
					.setParent(ProjectName.of(projectId).toString()).setInspectJob(inspectJobConfig).build();

			DlpJob dlpJob = dlpServiceClient.createDlpJob(createDlpJobRequest);

			System.out.println("Job created with ID:" + dlpJob.getName());

			final SettableApiFuture<Boolean> done = SettableApiFuture.create();

			// Set up a Pub/Sub subscriber to listen on the job completion status
			Subscriber subscriber = Subscriber.newBuilder(ProjectSubscriptionName.of(projectId, subscriptionId),
					(pubsubMessage, ackReplyConsumer) -> {
						if (pubsubMessage.getAttributesCount() > 0
								&& pubsubMessage.getAttributesMap().get("DlpJobName").equals(dlpJob.getName())) {
							// notify job completion
							done.set(true);
							ackReplyConsumer.ack();
						}
					}).build();
			subscriber.startAsync();

			// Wait for job completion semi-synchronously
			// For long jobs, consider using a truly asynchronous execution model such as
			// Cloud Functions
			try {
				done.get(3, TimeUnit.MINUTES);
				Thread.sleep(500); // Wait for the job to become available
			} catch (Exception e) {
				System.out.println("Unable to verify job completion.");
			}

			DlpJob completedJob = dlpServiceClient
					.getDlpJob(GetDlpJobRequest.newBuilder().setName(dlpJob.getName()).build());

			System.out.println("Job status: " + completedJob.getState());
			InspectDataSourceDetails inspectDataSourceDetails = completedJob.getInspectDetails();
			InspectDataSourceDetails.Result result = inspectDataSourceDetails.getResult();
			if (result.getInfoTypeStatsCount() > 0) {
				System.out.println("Findings: ");
				for (InfoTypeStats infoTypeStat : result.getInfoTypeStatsList()) {
					System.out.print("\tInfo type: " + infoTypeStat.getInfoType().getName());
					System.out.println("\tCount: " + infoTypeStat.getCount());
				}
			} else {
				System.out.println("No findings.");
			}
		}
	}
}
