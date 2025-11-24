'use client'

import { useState } from 'react'
import FileUpload from '@/components/FileUpload'
import ProcessingStatus from '@/components/ProcessingStatus'
import DownloadResults from '@/components/DownloadResults'
import ErrorDisplay from '@/components/ErrorDisplay'

export default function Home() {
  const [uploadId, setUploadId] = useState<number | null>(null)
  const [status, setStatus] = useState<string>('')
  const [error, setError] = useState<string | null>(null)

  const handleUploadSuccess = (id: number) => {
    setUploadId(id)
    setStatus('processing')
    setError(null)
  }

  const handleUploadError = (errorMessage: string) => {
    setError(errorMessage)
    setUploadId(null)
    setStatus('')
  }

  const handleStatusUpdate = (newStatus: string) => {
    setStatus(newStatus)
  }

  const handleReset = () => {
    setUploadId(null)
    setStatus('')
    setError(null)
  }

  return (
    <main className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-100 py-12 px-4 sm:px-6 lg:px-8">
      <div className="max-w-4xl mx-auto">
        {/* Header */}
        <div className="text-center mb-12">
          <h1 className="text-4xl font-bold text-gray-900 mb-4">
            GST Automation Tool
          </h1>
          <p className="text-lg text-gray-600">
            Upload your sales data and get GST-ready files automatically
          </p>
        </div>

        {/* Main Content */}
        <div className="bg-white rounded-2xl shadow-xl p-8">
          {error && (
            <ErrorDisplay error={error} onDismiss={() => setError(null)} />
          )}

          {!uploadId && !error && (
            <FileUpload
              onUploadSuccess={handleUploadSuccess}
              onUploadError={handleUploadError}
            />
          )}

          {uploadId && status === 'processing' && (
            <ProcessingStatus
              uploadId={uploadId}
              onStatusUpdate={handleStatusUpdate}
            />
          )}

          {uploadId && status === 'completed' && (
            <DownloadResults uploadId={uploadId} onReset={handleReset} />
          )}

          {uploadId && status === 'failed' && (
            <div className="text-center">
              <ErrorDisplay
                error="Processing failed. Please try again with a different file."
                onDismiss={handleReset}
              />
              <button
                onClick={handleReset}
                className="mt-4 px-6 py-3 bg-primary-600 text-white rounded-lg hover:bg-primary-700 transition-colors"
              >
                Upload Another File
              </button>
            </div>
          )}
        </div>

        {/* Features */}
        <div className="mt-12 grid grid-cols-1 md:grid-cols-3 gap-6">
          <div className="bg-white p-6 rounded-xl shadow-md">
            <div className="text-primary-600 mb-3">
              <svg className="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
              </svg>
            </div>
            <h3 className="text-lg font-semibold text-gray-900 mb-2">
              Smart Detection
            </h3>
            <p className="text-gray-600 text-sm">
              Automatically detects and validates GST numbers, invoice data, and amounts
            </p>
          </div>

          <div className="bg-white p-6 rounded-xl shadow-md">
            <div className="text-primary-600 mb-3">
              <svg className="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4" />
              </svg>
            </div>
            <h3 className="text-lg font-semibold text-gray-900 mb-2">
              Auto Classification
            </h3>
            <p className="text-gray-600 text-sm">
              Splits data into B2B, B2C, and Export categories automatically
            </p>
          </div>

          <div className="bg-white p-6 rounded-xl shadow-md">
            <div className="text-primary-600 mb-3">
              <svg className="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z" />
              </svg>
            </div>
            <h3 className="text-lg font-semibold text-gray-900 mb-2">
              Data Validation
            </h3>
            <p className="text-gray-600 text-sm">
              Validates all data against GST rules before generating output
            </p>
          </div>
        </div>
      </div>
    </main>
  )
}
