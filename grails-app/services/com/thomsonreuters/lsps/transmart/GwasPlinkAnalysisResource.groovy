package com.thomsonreuters.lsps.transmart

interface GwasPlinkAnalysisResource {

    void prepareZippedResult(String jobName, String analysisName)

    def getPreviewData(String jobName, String previewFileName, int previewRowsCount)

    def createAnalysisJob(Map params, String analysisGroup, Class jobClazz, boolean useAnalysisConstraints)
}