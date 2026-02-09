'use client'

import { useState, useEffect } from 'react'
import { useAuth } from '@/context/AuthContext'
import { useRouter } from 'next/navigation'
import GSTR2BService from '@/services/GSTR2BService'
import { Upload, FileSpreadsheet, CheckCircle, XCircle, Loader2, Download, ArrowLeft, Trash2 } from 'lucide-react'

export default function GSTR2BPage() {
  const { user, loading: authLoading } = useAuth()
  const router = useRouter()
  
  // File states
  const [purchaseFile, setPurchaseFile] = useState<File | null>(null)
  const [gstr2bFiles, setGstr2bFiles] = useState<File[]>([])
  
  // Processing states
  const [uploading, setUploading] = useState(false)
  const [processing, setProcessing] = useState(false)
  const [jobId, setJobId] = useState<number | null>(null)
  const [status, setStatus] = useState<string>('')
  const [error, setError] = useState<string | null>(null)
  const [statusData, setStatusData] = useState<any>(null)
  
  // Polling
  const [pollInterval, setPollInterval] = useState<NodeJS.Timeout | null>(null)

  useEffect(() => {
    if (!authLoading && !user) {
      router.push('/login')
    }
  }, [user, authLoading, router])

  useEffect(() => {
    // Cleanup polling on unmount
    return () => {
      if (pollInterval) {
        clearInterval(pollInterval)
      }
    }
  }, [pollInterval])

  if (authLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary-600"></div>
      </div>
    )
  }

  if (!user) {
    return null
  }

  const handlePurchaseFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files[0]) {
      setPurchaseFile(e.target.files[0])
      setError(null)
    }
  }

  const handleGSTR2BFilesChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files) {
      const filesArray = Array.from(e.target.files)
      setGstr2bFiles(prev => [...prev, ...filesArray])
      setError(null)
    }
  }

  const removeGSTR2BFile = (index: number) => {
    setGstr2bFiles(prev => prev.filter((_, i) => i !== index))
  }

  const handleProcess = async () => {
    if (!purchaseFile) {
      setError('Please upload a purchase book file')
      return
    }

    if (gstr2bFiles.length === 0) {
      setError('Please upload at least one GSTR2B file')
      return
    }

    try {
      setUploading(true)
      setError(null)

      // Upload purchase book
      await GSTR2BService.uploadPurchaseBook(purchaseFile)

      // Upload GSTR2B files
      await GSTR2BService.uploadGSTR2BFiles(gstr2bFiles)

      // Trigger processing
      const result = await GSTR2BService.processReconciliation()
      
      setJobId(result.job_id)
      setStatus('processing')
      setProcessing(true)
      setUploading(false)

      // Start polling for status
      const interval = setInterval(async () => {
        try {
          const statusResponse = await GSTR2BService.getJobStatus(result.job_id)
          setStatusData(statusResponse)
          setStatus(statusResponse.status.toLowerCase())

          if (statusResponse.status === 'completed' || statusResponse.status === 'failed') {
            clearInterval(interval)
            setProcessing(false)
            
            if (statusResponse.status === 'failed') {
              setError(statusResponse.error_message || 'Processing failed')
            }
          }
        } catch (err: any) {
          console.error('Error polling status:', err)
        }
      }, 3000) // Poll every 3 seconds

      setPollInterval(interval)

    } catch (err: any) {
      console.error('Error processing:', err)
      setError(err.response?.data?.detail || err.message || 'An error occurred')
      setUploading(false)
      setProcessing(false)
    }
  }

  const handleDownload = async () => {
    if (!jobId) return

    try {
      const blob = await GSTR2BService.downloadReport(jobId)
      const filename = `GSTR2B_Reconciliation_${jobId}_${new Date().toISOString().split('T')[0]}.xlsx`
      GSTR2BService.downloadBlob(blob, filename)
    } catch (err: any) {
      console.error('Error downloading:', err)
      setError('Failed to download report')
    }
  }

  const handleReset = () => {
    setPurchaseFile(null)
    setGstr2bFiles([])
    setJobId(null)
    setStatus('')
    setError(null)
    setStatusData(null)
    setProcessing(false)
    setUploading(false)
    if (pollInterval) {
      clearInterval(pollInterval)
      setPollInterval(null)
    }
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-100">
      {/* Header */}
      <header className="bg-white shadow-sm">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
          <div className="flex items-center space-x-4">
            <button
              onClick={() => router.push('/dashboard')}
              className="p-2 hover:bg-gray-100 rounded-lg transition-colors"
            >
              <ArrowLeft className="w-5 h-5 text-gray-600" />
            </button>
            <div>
              <h1 className="text-2xl font-bold text-gray-900">GSTR2B Reconciliation</h1>
              <p className="text-sm text-gray-600">Upload purchase books and GSTR2B files for reconciliation</p>
            </div>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <div className="max-w-5xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="bg-white rounded-2xl shadow-xl p-8">
          
          {error && (
            <div className="mb-6 p-4 bg-red-50 border border-red-200 rounded-lg flex items-start space-x-3">
              <XCircle className="w-5 h-5 text-red-600 flex-shrink-0 mt-0.5" />
              <div>
                <h3 className="text-sm font-medium text-red-800">Error</h3>
                <p className="text-sm text-red-700 mt-1">{error}</p>
              </div>
              <button
                onClick={() => setError(null)}
                className="ml-auto text-red-600 hover:text-red-800"
              >
                ×
              </button>
            </div>
          )}

          {!processing && !status && (
            <div className="space-y-6">
              {/* Purchase Book Upload */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Purchase Book (Excel)
                </label>
                <div className="border-2 border-dashed border-gray-300 rounded-lg p-6 hover:border-primary-500 transition-colors">
                  <div className="flex flex-col items-center space-y-2">
                    <Upload className="w-10 h-10 text-gray-400" />
                    <div className="text-center">
                      <label htmlFor="purchase-upload" className="cursor-pointer">
                        <span className="text-primary-600 hover:text-primary-700 font-medium">
                          Click to upload
                        </span>
                        <span className="text-gray-600"> or drag and drop</span>
                      </label>
                      <input
                        id="purchase-upload"
                        type="file"
                        className="hidden"
                        accept=".xlsx,.xls"
                        onChange={handlePurchaseFileChange}
                      />
                    </div>
                    <p className="text-xs text-gray-500">Excel files only</p>
                  </div>

                  {purchaseFile && (
                    <div className="mt-4 p-3 bg-green-50 border border-green-200 rounded-lg flex items-center justify-between">
                      <div className="flex items-center space-x-2">
                        <FileSpreadsheet className="w-5 h-5 text-green-600" />
                        <span className="text-sm font-medium text-green-900">{purchaseFile.name}</span>
                        <span className="text-xs text-green-700">
                          ({(purchaseFile.size / 1024 / 1024).toFixed(2)} MB)
                        </span>
                      </div>
                      <button
                        onClick={() => setPurchaseFile(null)}
                        className="text-red-600 hover:text-red-800"
                      >
                        <Trash2 className="w-4 h-4" />
                      </button>
                    </div>
                  )}
                </div>
              </div>

              {/* GSTR2B Files Upload */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  GSTR2B Files (Multiple allowed - one per state)
                </label>
                <div className="border-2 border-dashed border-gray-300 rounded-lg p-6 hover:border-primary-500 transition-colors">
                  <div className="flex flex-col items-center space-y-2">
                    <Upload className="w-10 h-10 text-gray-400" />
                    <div className="text-center">
                      <label htmlFor="gstr2b-upload" className="cursor-pointer">
                        <span className="text-primary-600 hover:text-primary-700 font-medium">
                          Click to upload
                        </span>
                        <span className="text-gray-600"> or drag and drop</span>
                      </label>
                      <input
                        id="gstr2b-upload"
                        type="file"
                        className="hidden"
                        accept=".xlsx,.xls"
                        multiple
                        onChange={handleGSTR2BFilesChange}
                      />
                    </div>
                    <p className="text-xs text-gray-500">Excel files only • Multiple files supported</p>
                  </div>

                  {gstr2bFiles.length > 0 && (
                    <div className="mt-4 space-y-2">
                      <p className="text-sm font-medium text-gray-700">{gstr2bFiles.length} file(s) selected:</p>
                      {gstr2bFiles.map((file, index) => (
                        <div
                          key={index}
                          className="p-3 bg-blue-50 border border-blue-200 rounded-lg flex items-center justify-between"
                        >
                          <div className="flex items-center space-x-2">
                            <FileSpreadsheet className="w-5 h-5 text-blue-600" />
                            <span className="text-sm font-medium text-blue-900">{file.name}</span>
                            <span className="text-xs text-blue-700">
                              ({(file.size / 1024 / 1024).toFixed(2)} MB)
                            </span>
                          </div>
                          <button
                            onClick={() => removeGSTR2BFile(index)}
                            className="text-red-600 hover:text-red-800"
                          >
                            <Trash2 className="w-4 h-4" />
                          </button>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              </div>

              {/* Process Button */}
              <div className="pt-4">
                <button
                  onClick={handleProcess}
                  disabled={uploading || !purchaseFile || gstr2bFiles.length === 0}
                  className="w-full py-3 px-6 bg-primary-600 text-white font-medium rounded-lg hover:bg-primary-700 disabled:bg-gray-300 disabled:cursor-not-allowed transition-colors flex items-center justify-center space-x-2"
                >
                  {uploading ? (
                    <>
                      <Loader2 className="w-5 h-5 animate-spin" />
                      <span>Uploading & Processing...</span>
                    </>
                  ) : (
                    <>
                      <CheckCircle className="w-5 h-5" />
                      <span>Process Reconciliation</span>
                    </>
                  )}
                </button>
              </div>
            </div>
          )}

          {/* Processing Status */}
          {processing && (
            <div className="text-center py-12">
              <Loader2 className="w-16 h-16 text-primary-600 animate-spin mx-auto mb-4" />
              <h3 className="text-xl font-semibold text-gray-900 mb-2">Processing Reconciliation</h3>
              <p className="text-gray-600">
                Reconciling {statusData?.purchase_filename} with {statusData?.gstr2b_filenames?.length || gstr2bFiles.length} GSTR2B file(s)
              </p>
              <p className="text-sm text-gray-500 mt-2">This may take a few moments...</p>
            </div>
          )}

          {/* Completed Status */}
          {status === 'completed' && statusData && (
            <div className="text-center py-12">
              <CheckCircle className="w-16 h-16 text-green-600 mx-auto mb-4" />
              <h3 className="text-xl font-semibold text-gray-900 mb-2">Reconciliation Completed!</h3>
              <p className="text-gray-600 mb-6">Your reconciliation report is ready to download.</p>

              {statusData.processing_metadata?.overall && (
                <div className="bg-gray-50 rounded-lg p-6 mb-6">
                  <h4 className="text-lg font-medium text-gray-900 mb-4">Summary</h4>
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                    <div className="bg-white rounded-lg p-4 shadow-sm">
                      <p className="text-2xl font-bold text-green-600">{statusData.processing_metadata.overall.matched}</p>
                      <p className="text-sm text-gray-600">Matched</p>
                    </div>
                    <div className="bg-white rounded-lg p-4 shadow-sm">
                      <p className="text-2xl font-bold text-yellow-600">{statusData.processing_metadata.overall.not_matched}</p>
                      <p className="text-sm text-gray-600">Not Matched</p>
                    </div>
                    <div className="bg-white rounded-lg p-4 shadow-sm">
                      <p className="text-2xl font-bold text-red-600">{statusData.processing_metadata.overall.not_in_books}</p>
                      <p className="text-sm text-gray-600">Not in Books</p>
                    </div>
                    <div className="bg-white rounded-lg p-4 shadow-sm">
                      <p className="text-2xl font-bold text-orange-600">{statusData.processing_metadata.overall.not_in_2b}</p>
                      <p className="text-sm text-gray-600">Not in 2B</p>
                    </div>
                  </div>
                </div>
              )}

              <div className="flex space-x-4 justify-center">
                <button
                  onClick={handleDownload}
                  className="px-6 py-3 bg-primary-600 text-white font-medium rounded-lg hover:bg-primary-700 transition-colors flex items-center space-x-2"
                >
                  <Download className="w-5 h-5" />
                  <span>Download Report</span>
                </button>
                <button
                  onClick={handleReset}
                  className="px-6 py-3 border border-gray-300 text-gray-700 font-medium rounded-lg hover:bg-gray-50 transition-colors"
                >
                  Process Another
                </button>
              </div>
            </div>
          )}

          {/* Failed Status */}
          {status === 'failed' && (
            <div className="text-center py-12">
              <XCircle className="w-16 h-16 text-red-600 mx-auto mb-4" />
              <h3 className="text-xl font-semibold text-gray-900 mb-2">Processing Failed</h3>
              <p className="text-gray-600 mb-6">{statusData?.error_message || 'An error occurred during processing'}</p>
              <button
                onClick={handleReset}
                className="px-6 py-3 bg-primary-600 text-white font-medium rounded-lg hover:bg-primary-700 transition-colors"
              >
                Try Again
              </button>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
