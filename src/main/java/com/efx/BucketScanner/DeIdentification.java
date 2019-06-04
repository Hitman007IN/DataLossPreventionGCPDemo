package com.efx.BucketScanner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.privacy.dlp.v2.CharacterMaskConfig;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.DeidentifyConfig;
import com.google.privacy.dlp.v2.DeidentifyContentRequest;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.InfoType;
import com.google.privacy.dlp.v2.InfoTypeTransformations;
import com.google.privacy.dlp.v2.InfoTypeTransformations.InfoTypeTransformation;
import com.google.privacy.dlp.v2.InspectConfig;
import com.google.privacy.dlp.v2.PrimitiveTransformation;
import com.google.privacy.dlp.v2.ProjectName;
import com.google.protobuf.TextFormat.ParseException;
import com.google.cloud.ServiceOptions;

import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;

public class DeIdentification {

	private static void deIdentifyWithMask(String string, List<InfoType> infoTypes, Character maskingCharacter,
			int numberToMask, String projectId) {

		// instantiate a client
		try (DlpServiceClient dlpServiceClient = DlpServiceClient.create()) {

			ContentItem contentItem = ContentItem.newBuilder().setValue(string).build();

			CharacterMaskConfig characterMaskConfig = CharacterMaskConfig.newBuilder()
					.setMaskingCharacter(maskingCharacter.toString()).setNumberToMask(numberToMask).build();

			// Create the deidentification transformation configuration
			PrimitiveTransformation primitiveTransformation = PrimitiveTransformation.newBuilder()
					.setCharacterMaskConfig(characterMaskConfig).build();

			InfoTypeTransformation infoTypeTransformationObject = InfoTypeTransformation.newBuilder()
					.setPrimitiveTransformation(primitiveTransformation).build();

			InfoTypeTransformations infoTypeTransformationArray = InfoTypeTransformations.newBuilder()
					.addTransformations(infoTypeTransformationObject).build();

			InspectConfig inspectConfig = InspectConfig.newBuilder().addAllInfoTypes(infoTypes).build();

			DeidentifyConfig deidentifyConfig = DeidentifyConfig.newBuilder()
					.setInfoTypeTransformations(infoTypeTransformationArray).build();

			// Create the deidentification request object
			DeidentifyContentRequest request = DeidentifyContentRequest.newBuilder()
					.setParent(ProjectName.of(projectId).toString()).setInspectConfig(inspectConfig)
					.setDeidentifyConfig(deidentifyConfig).setItem(contentItem).build();

			// Execute the deidentification request
			DeidentifyContentResponse response = dlpServiceClient.deidentifyContent(request);

			// Print the character-masked input value
			// e.g. "My SSN is 123456789" --> "My SSN is *********"
			String result = response.getItem().getValue();
			System.out.println(result);
		} catch (Exception e) {
			System.out.println("Error in deidentifyWithMask: " + e.getMessage());
		}
	}
	
	public static void main(String[] args) throws Exception {

	    OptionGroup optionsGroup = new OptionGroup();
	    optionsGroup.setRequired(true);

//	    Option deidentifyReplaceWithInfoTypeOption =
//	        new Option("it", "info_type_replace", true, "Deidentify by replacing with info type.");
//	    optionsGroup.addOption(deidentifyReplaceWithInfoTypeOption);

	    Option deidentifyMaskingOption =
	        new Option("m", "mask", true, "Deidentify with character masking.");
	    optionsGroup.addOption(deidentifyMaskingOption);

//	    Option deidentifyFpeOption =
//	        new Option("f", "fpe", true, "Deidentify with format-preserving encryption.");
//	    optionsGroup.addOption(deidentifyFpeOption);
//
//	    Option reidentifyFpeOption =
//	        new Option("r", "reid", true, "Reidentify with format-preserving encryption.");
//	    optionsGroup.addOption(reidentifyFpeOption);
//
//	    Option deidentifyDateShiftOption =
//	        new Option("d", "date", false, "Deidentify dates in a CSV file.");
//	    optionsGroup.addOption(deidentifyDateShiftOption);

	    Options commandLineOptions = new Options();
	    commandLineOptions.addOptionGroup(optionsGroup);

	    Option infoTypesOption = Option.builder("infoTypes").hasArg(true).required(false).build();
	    infoTypesOption.setArgs(Option.UNLIMITED_VALUES);
	    commandLineOptions.addOption(infoTypesOption);

	    Option maskingCharacterOption =
	        Option.builder("maskingCharacter").hasArg(true).required(false).build();
	    commandLineOptions.addOption(maskingCharacterOption);

	    Option surrogateTypeOption =
	        Option.builder("surrogateType").hasArg(true).required(false).build();
	    commandLineOptions.addOption(surrogateTypeOption);

	    Option numberToMaskOption = Option.builder("numberToMask").hasArg(true).required(false).build();
	    commandLineOptions.addOption(numberToMaskOption);

	    Option alphabetOption = Option.builder("commonAlphabet").hasArg(true).required(false).build();
	    commandLineOptions.addOption(alphabetOption);

	    Option wrappedKeyOption = Option.builder("wrappedKey").hasArg(true).required(false).build();
	    commandLineOptions.addOption(wrappedKeyOption);

	    Option keyNameOption = Option.builder("keyName").hasArg(true).required(false).build();
	    commandLineOptions.addOption(keyNameOption);

	    Option inputCsvPathOption = Option.builder("inputCsvPath").hasArg(true).required(false).build();
	    commandLineOptions.addOption(inputCsvPathOption);

	    Option outputCsvPathOption =
	        Option.builder("outputCsvPath").hasArg(true).required(false).build();
	    commandLineOptions.addOption(outputCsvPathOption);

	    Option dateFieldsOption = Option.builder("dateFields").hasArg(true).required(false).build();
	    commandLineOptions.addOption(dateFieldsOption);

	    Option lowerBoundDaysOption =
	        Option.builder("lowerBoundDays").hasArg(true).required(false).build();
	    commandLineOptions.addOption(lowerBoundDaysOption);

	    Option upperBoundDaysOption =
	        Option.builder("upperBoundDays").hasArg(true).required(false).build();
	    commandLineOptions.addOption(upperBoundDaysOption);

	    Option contextFieldNameOption =
	        Option.builder("contextField").hasArg(true).required(false).build();
	    commandLineOptions.addOption(contextFieldNameOption);

	    Option projectIdOption = Option.builder("projectId").hasArg(true).required(false).build();
	    commandLineOptions.addOption(projectIdOption);

	    CommandLineParser parser = new DefaultParser();
	    HelpFormatter formatter = new HelpFormatter();
	    CommandLine cmd;

	    cmd = parser.parse(commandLineOptions, args);

	    // default to auto-detected project id when not explicitly provided
	    String projectId =
	        cmd.getOptionValue(projectIdOption.getOpt(), ServiceOptions.getDefaultProjectId());

	    List<InfoType> infoTypesList = Collections.emptyList();
	    if (cmd.hasOption(infoTypesOption.getOpt())) {
	      infoTypesList = new ArrayList<>();
	      String[] infoTypes = cmd.getOptionValues(infoTypesOption.getOpt());
	      for (String infoType : infoTypes) {
	        infoTypesList.add(InfoType.newBuilder().setName(infoType).build());
	      }
	    }

	    if (cmd.hasOption("m")) {
	      // deidentification with character masking
	      int numberToMask = Integer.parseInt(cmd.getOptionValue(numberToMaskOption.getOpt(), "0"));
	      char maskingCharacter = cmd.getOptionValue(maskingCharacterOption.getOpt(), "*").charAt(0);
	      String val = cmd.getOptionValue(deidentifyMaskingOption.getOpt());
	      deIdentifyWithMask(val, infoTypesList, maskingCharacter, numberToMask, projectId);
	    }
	}
}
