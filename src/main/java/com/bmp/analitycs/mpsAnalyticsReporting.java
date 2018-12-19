package com.bmp.analitycs;


import com.google.api.client.googleapis.GoogleUtils;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.security.GeneralSecurityException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import com.google.api.services.analyticsreporting.v4.AnalyticsReportingScopes;
import com.google.api.services.analyticsreporting.v4.AnalyticsReporting;

import com.google.api.services.analyticsreporting.v4.model.ColumnHeader;
import com.google.api.services.analyticsreporting.v4.model.DateRange;
import com.google.api.services.analyticsreporting.v4.model.DateRangeValues;
import com.google.api.services.analyticsreporting.v4.model.GetReportsRequest;
import com.google.api.services.analyticsreporting.v4.model.GetReportsResponse;
import com.google.api.services.analyticsreporting.v4.model.Metric;
import com.google.api.services.analyticsreporting.v4.model.Dimension;
import com.google.api.services.analyticsreporting.v4.model.MetricHeaderEntry;
import com.google.api.services.analyticsreporting.v4.model.Report;
import com.google.api.services.analyticsreporting.v4.model.ReportRequest;
import com.google.api.services.analyticsreporting.v4.model.ReportRow;

public class mpsAnalyticsReporting {
    private static final String APPLICATION_NAME = "MPS - REPORT EXTRACTOR";
    private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
    private static final String KEY_FILE_LOCATION = "key_file.json";
    //private static final String VIEW_ID_MPS = "173042596";
    private static final String VIEW_ID_MPS = "173042596";
    private static final String VIEW_ID_DIGITAL = "172512857";

    public static void main(String[] args) {
        try {
            AnalyticsReporting service = initializeAnalyticsReporting();
//            args = new String[4];
//                args[0]="yesterday";
//                args[1]="today";
//                args[2]="2";
//                args[3]="/Users/luchian.bordiyan/Downloads";
            if (args.length <= 0 || args.length != 4) {
                System.exit(0);
            } else {
                String type = args[2];
                if (type.equals("1")) {
                    GetReportsResponse response = getReport(service, VIEW_ID_DIGITAL, args[0], args[1], args[2], false);
                    printResponse(response, args[3], args[2], false);
                } else {
                    GetReportsResponse response = getReport(service, VIEW_ID_DIGITAL, args[0], args[1], args[2], false);
                    printResponse(response, args[3], args[2], false);
                    GetReportsResponse response2 = getReport(service, VIEW_ID_MPS, args[0], args[1], args[2], true);
                    printResponse(response2, args[3], args[2], true);
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Initializes an Analytics Reporting API V4 service object.
     *
     * @return An authorized Analytics Reporting API V4 service object.
     * @throws IOException
     * @throws GeneralSecurityException
     */

    static HttpTransport newProxyTransport() throws GeneralSecurityException, IOException {
        NetHttpTransport.Builder builder = new NetHttpTransport.Builder();
        builder.trustCertificates(GoogleUtils.getCertificateTrustStore());
        builder.setProxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress("proxy.testfactory.copergmps", 80)));
        return builder.build();
    }


    private static AnalyticsReporting initializeAnalyticsReporting() throws GeneralSecurityException, IOException {

        HttpTransport httpTransport = newProxyTransport();

        //URL resource = HelloAnalyticsReporting.class.getResource(KEY_FILE_LOCATION);
        GoogleCredential credential = GoogleCredential
                .fromStream(mpsAnalyticsReporting.class.getClassLoader().getResourceAsStream(KEY_FILE_LOCATION))
                .createScoped(AnalyticsReportingScopes.all());

        return new AnalyticsReporting.Builder(httpTransport, JSON_FACTORY, credential)
                .setApplicationName(APPLICATION_NAME).build();
    }

    /**
     * Queries the Analytics Reporting API V4.
     *
     * @param service An authorized Analytics Reporting API V4 service object.
     * @return GetReportResponse The Analytics Reporting API V4 response.
     * @throws IOException
     */


    private static GetReportsResponse getReport(AnalyticsReporting service, String view_id, String startDate, String endDate, String type, Boolean secondFile) throws IOException {
        // Create the DateRange object.
        DateRange dateRange = new DateRange();
        dateRange.setStartDate(startDate);
        dateRange.setEndDate(endDate);
        ReportRequest request;

        if (type.equals("1")) {
            Metric metric1 = new Metric()
                    .setExpression("ga:totalEvents")
                    .setAlias("totale_eventi");
            Metric metric2 = new Metric()
                    .setExpression("ga:uniqueEvents")
                    .setAlias("eventi_unici");

            Dimension dim1 = new Dimension().setName("ga:dimension9");
            Dimension dim2 = new Dimension().setName("ga:date");
            Dimension dim3 = new Dimension().setName("ga:contentGroup1");
            Dimension dim4 = new Dimension().setName("ga:eventLabel");
            Dimension dim5 = new Dimension().setName("ga:sourceMedium");

            // Create the ReportRequest object.
            request = new ReportRequest()
                    .setViewId(view_id)
                    .setDateRanges(Arrays.asList(dateRange))
                    .setMetrics(Arrays.asList(metric1, metric2))
                    .setDimensions(Arrays.asList(dim1, dim2, dim3, dim4, dim5));

            ArrayList<ReportRequest> requests = new ArrayList<ReportRequest>();
            requests.add(request);


            // Create the GetReportsRequest object.
            GetReportsRequest getReport = new GetReportsRequest()
                    .setReportRequests(requests);

            // Call the batchGet method.
            GetReportsResponse response = service.reports().batchGet(getReport).execute();

            // Return the response.
            return response;
        } else {
            // Create the Metrics object.
            Metric metric1 = new Metric()
                    .setExpression("ga:sessions")
                    .setAlias("sessioni");

            Metric metric2 = new Metric()
                    .setExpression("ga:avgSessionDuration")
                    .setAlias("durata_sessione_media");

            Metric metric3 = new Metric()
                    .setExpression("ga:pageviewsPerSession")
                    .setAlias("pagine_session");

            Metric metric4 = new Metric()
                    .setExpression("ga:avgSessionDuration")
                    .setAlias("tempo_medio_sulla_pagina");

            Metric metric5 = new Metric()
                    .setExpression("ga:avgSessionDuration")
                    .setAlias("frequenza_di_rimbalzo");

            Dimension dim1 = new Dimension().setName("ga:date");
            Dimension sessionID;
            if (secondFile) {
                sessionID = new Dimension().setName("ga:dimension13");
            } else {
                sessionID = new Dimension().setName("ga:dimension10");
            }
            Dimension userID;
            if (secondFile) {
                userID = new Dimension().setName("ga:dimension11");
            } else {
                userID = new Dimension().setName("ga:dimension9");
            }


            // Create the ReportRequest object.
            request = new ReportRequest()
                    .setViewId(view_id)
                    .setDateRanges(Arrays.asList(dateRange))
                    .setMetrics(Arrays.asList(metric1, metric2, metric3, metric4, metric5))
                    .setDimensions(Arrays.asList(sessionID, userID, dim1));

            ArrayList<ReportRequest> requests = new ArrayList<ReportRequest>();
            requests.add(request);

            // Create the GetReportsRequest object.
            GetReportsRequest getReport = new GetReportsRequest()
                    .setReportRequests(requests);

            // Call the batchGet method.
            GetReportsResponse response = service.reports().batchGet(getReport).execute();

            // Return the response.
            return response;
        }

    }

    /**
     * Parses and prints the Analytics Reporting API V4 response.
     *
     * @param response An Analytics Reporting API V4 response.
     */
    private static void printResponse(GetReportsResponse response, String path, String type, Boolean secondFile) throws IOException {
        String currentFDate = getCurrentFormatedDate();
        String fileType = type.equals("1") ? "/MKB.BAT.GOOGLEA" : "/MKB.BAT.GOOGLEB";
        if (secondFile) {
            fileType += "2DIGITAL.";
        } else {
            fileType += ".";
        }
        String fileName = fileType + currentFDate + ".csv";
        File file = new File(path + fileName);
        file.createNewFile();
        PrintWriter writer = new PrintWriter(file);
        List<String> headers = new ArrayList<String>();

        for (Report report : response.getReports()) {
            ColumnHeader header = report.getColumnHeader();
            List<String> dimensionHeaders = header.getDimensions();
            List<MetricHeaderEntry> metricHeaders = header.getMetricHeader().getMetricHeaderEntries();
            List<ReportRow> rows = report.getData().getRows();

            for (int i = 0; i < dimensionHeaders.size(); i++) {
                if (dimensionHeaders.get(i).equals("ga:dimension9") || dimensionHeaders.get(i).equals("ga:dimension11")) {
                    dimensionHeaders.set(i, "user_id");
                }
                if (dimensionHeaders.get(i).equals("ga:dimension10") || dimensionHeaders.get(i).equals("ga:dimension13")) {
                    dimensionHeaders.set(i, "session_id");
                }
                if (dimensionHeaders.get(i).equals("ga:date")) {
                    dimensionHeaders.set(i, "data");
                }
                if (dimensionHeaders.get(i).equals("ga:contentGroup1")) {
                    dimensionHeaders.set(i, "section_grouping");
                }
                if (dimensionHeaders.get(i).equals("ga:eventLabel")) {
                    dimensionHeaders.set(i, "event_full_label");
                }
                if (dimensionHeaders.get(i).equals("ga:sourceMedium")) {
                    dimensionHeaders.set(i, "sorgente_mezzo");
                }
            }

            for (int i = 0; i < metricHeaders.size(); i++) {
                dimensionHeaders.add(metricHeaders.get(i).getName());
            }

            String collectDimensions = strJoin(dimensionHeaders, ",");
            writer.write(collectDimensions);
//            String collectMetrics = metricHeaders.stream().collect(Collectors.joining(","));
//            writer.write(collectMetrics);

            if (rows == null) {
                System.out.println("No data found for " + VIEW_ID_MPS);
                writer.close();
                return;
            }

            for (ReportRow row : rows) {
                List<String> dimensions = row.getDimensions();
                List<DateRangeValues> metrics = row.getMetrics();
                writer.write(System.getProperty("line.separator"));

                for (int i = 0; i < dimensions.size(); i++) {
                    //System.out.println(dimensionHeaders.get(i) + ": " + dimensions.get(i));
                    writer.write(dimensions.get(i));
                    writer.write(",");
                    //writer.println(dimensionHeaders.get(i) + ": " + dimensions.get(i));
                }

                for (int j = 0; j < metrics.size(); j++) {
                    //System.out.print("Date Range (" + j + "): ");
                    //writer.println("Date Range (" + j + "): ");
                    DateRangeValues values = metrics.get(j);
                    for (int k = 0; k < values.getValues().size() && k < metricHeaders.size(); k++) {
                        writer.write(values.getValues().get(k));
                        if (values.getValues().size() != k + 1) {
                            writer.write(",");
                        }
                        //System.out.println(metricHeaders.get(k).getName() + ": " + values.getValues().get(k));
                        //headers.add(metricHeaders.get(k).getName());
                        //writer.println(metricHeaders.get(k).getName() + ": " + values.getValues().get(k));
                    }
                }
            }
        }
        //System.out.println(collect);
        writer.close();
    }

    public static String getCurrentFormatedDate() {
        String result;
        Date date = new Date();
        DateFormat yyf = new SimpleDateFormat("yy");
        DateFormat mmf = new SimpleDateFormat("MM");
        DateFormat ddf = new SimpleDateFormat("dd");
        DateFormat hoursf = new SimpleDateFormat("HH");
        DateFormat minutesf = new SimpleDateFormat("mm");
        DateFormat secondsf = new SimpleDateFormat("ss");
        String yy = yyf.format(date);
        String mm = mmf.format(date);
        String dd = ddf.format(date);
        String hours = hoursf.format(date);
        String minutes = minutesf.format(date);
        String seconds = secondsf.format(date);
        result = "D" + yy + mm + dd + "T" + hours + minutes + seconds;
        return result;
    }

    public static String strJoin(List<String> aArr, String sSep) {
        StringBuilder sbStr = new StringBuilder();
        for (int i = 0, il = aArr.size(); i < il; i++) {
            if (i > 0)
                sbStr.append(sSep);
            sbStr.append(aArr.get(i));
        }
        return sbStr.toString();
    }
}
