package com.thomsonreuters.lsps.transmart

import com.thomsonreuters.lsps.transmart.jobs.GwasPlinkAnalysisJob
import org.codehaus.groovy.grails.web.json.JSONArray
import org.codehaus.groovy.grails.web.json.JSONObject

class GwasPlinkController {
    def gwasPlinkAnalysisService
    def jobsAccessChecksService

    def show() {
    }

    def resultOutput() {
        int previewRowsCount = (params.previewRowsCount ?: 10).toInteger()
        def zipLink = "/gwasPlink/streamZip?jobName=${params.jobName}&analysisName=${params.analysisName}"
        [
                previewData     : gwasPlinkAnalysisService.getPreviewData(params.jobName as String, params.previewFileName as String, previewRowsCount),
                zipLink         : zipLink,
                previewRowsCount: previewRowsCount
        ]
    }

    def scheduleJob() {
        def result = gwasPlinkAnalysisService.createAnalysisJob(params, 'GWASPlink', GwasPlinkAnalysisJob, false)
        render text: result.toString(), contentType: 'application/json'
    }

    def streamZip() {
        String analysisName = params.analysisName
        String jobName = params.jobName
        if (!jobsAccessChecksService.canDownload(jobName)) {
            render status: 403
            return
        }
        response.setHeader('Content-disposition', "attachment; filename=${analysisName}.zip")
        response.contentType = 'application/zip'
        gwasPlinkAnalysisService.zipOutputStream(response.outputStream, jobName, analysisName)
        response.outputStream.flush()
    }

    def loadScripts() {
        JSONObject result = new JSONObject()
        JSONArray rows = new JSONArray()

        ['gwasPlinkAdd.js'].each {
            JSONObject aScript = new JSONObject()
            aScript.put("path", "${servletContext.contextPath}${pluginContextPath}/js/${it}" as String)
            aScript.put("type", "script")
            rows.put(aScript)
        }
        [].each {
            JSONObject aStylesheet = new JSONObject()
            aStylesheet.put("path", "${servletContext.contextPath}${pluginContextPath}/css/${it}" as String)
            aStylesheet.put("type", "stylesheet")
            rows.put(aStylesheet)
        }

        result.put("success", true)
        result.put("totalCount", rows.size())
        result.put("files", rows)

        response.setContentType("text/json")
        response.outputStream << result.toString()
    }
}
