'use client'

import { Download, FileSpreadsheet, RefreshCw } from 'lucide-react'
import { downloadFile } from '@/services/api'

interface DownloadResultsProps {
  uploadId: number
  onReset: () => void
}

export default function DownloadResults({ uploadId, onReset }: DownloadResultsProps) {
  const handleDownload = async () => {
    try {
      const blob = await downloadFile(uploadId)
      const url = window.URL.createObjectURL(blob)
      const link = document.createElement('a')
      link.href = url
      link.download = `GST_Processed_${Date.now()}.xlsx`
      document.body.appendChild(link)
      link.click()
      document.body.removeChild(link)
      window.URL.revokeObjectURL(url)
    } catch (error) {
      console.error('Download error:', error)
      alert('Failed to download file. Please try again.')
    }
  }

  return (
    <div className="text-center">
      <div className="mb-8">
        <div className="inline-flex items-center justify-center w-20 h-20 bg-green-100 rounded-full mb-4">
          <FileSpreadsheet className="w-10 h-10 text-green-600" />
        </div>
        <h2 className="text-2xl font-bold text-gray-900 mb-2">
          Your GST File is Ready!
        </h2>
        <p className="text-gray-600">
          Your sales data has been processed and organized into GST-compliant format
        </p>
      </div>

      <div className="space-y-4">
        <button
          onClick={handleDownload}
          className="w-full inline-flex items-center justify-center px-6 py-4 bg-primary-600 text-white rounded-lg hover:bg-primary-700 transition-colors font-medium"
        >
          <Download className="w-5 h-5 mr-2" />
          Download Processed File
        </button>

        <button
          onClick={onReset}
          className="w-full inline-flex items-center justify-center px-6 py-3 border-2 border-gray-300 text-gray-700 rounded-lg hover:border-primary-500 hover:text-primary-600 transition-colors"
        >
          <RefreshCw className="w-4 h-4 mr-2" />
          Process Another File
        </button>
      </div>

      <div className="mt-8 p-6 bg-blue-50 rounded-lg text-left">
        <h3 className="font-semibold text-gray-900 mb-3">What's in your file:</h3>
        <ul className="space-y-2 text-sm text-gray-700">
          <li className="flex items-start">
            <span className="text-primary-600 mr-2">✓</span>
            <span>B2B transactions with validated GSTIN numbers</span>
          </li>
          <li className="flex items-start">
            <span className="text-primary-600 mr-2">✓</span>
            <span>B2C transactions properly categorized</span>
          </li>
          <li className="flex items-start">
            <span className="text-primary-600 mr-2">✓</span>
            <span>Export transactions (if any) in separate sheet</span>
          </li>
          <li className="flex items-start">
            <span className="text-primary-600 mr-2">✓</span>
            <span>All data validated against GST rules</span>
          </li>
        </ul>
      </div>
    </div>
  )
}
