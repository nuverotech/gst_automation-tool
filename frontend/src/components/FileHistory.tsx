'use client'

import { useEffect, useState } from 'react'
import { getUserUploads, downloadFile } from '@/services/api'
import { UploadResponse } from '@/types'
import { FileSpreadsheet, Download, Clock, CheckCircle, XCircle, Loader2 } from 'lucide-react'

export default function FileHistory() {
  const [uploads, setUploads] = useState<UploadResponse[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  useEffect(() => {
    fetchUploads()
  }, [])

  const fetchUploads = async () => {
    try {
      const data = await getUserUploads()
      setUploads(data)
    } catch (err: any) {
      console.error('Error fetching uploads:', err)
      setError('Failed to load file history')
    } finally {
      setLoading(false)
    }
  }

  const handleDownload = async (uploadId: number, filename: string) => {
    try {
      const blob = await downloadFile(uploadId)
      const url = window.URL.createObjectURL(blob)
      const link = document.createElement('a')
      link.href = url
      link.download = `GST_${filename}`
      document.body.appendChild(link)
      link.click()
      document.body.removeChild(link)
      window.URL.revokeObjectURL(url)
    } catch (error) {
      console.error('Download error:', error)
      alert('Failed to download file')
    }
  }

  const getStatusIcon = (status: string) => {
    const statusLower = status.toLowerCase()
    
    if (statusLower === 'completed') {
      return <CheckCircle className="w-5 h-5 text-green-600" />
    } else if (statusLower === 'failed') {
      return <XCircle className="w-5 h-5 text-red-600" />
    } else if (statusLower === 'processing') {
      return <Loader2 className="w-5 h-5 text-blue-600 animate-spin" />
    } else {
      return <Clock className="w-5 h-5 text-gray-400" />
    }
  }

  const getStatusColor = (status: string) => {
    const statusLower = status.toLowerCase()
    
    if (statusLower === 'completed') {
      return 'bg-green-100 text-green-800'
    } else if (statusLower === 'failed') {
      return 'bg-red-100 text-red-800'
    } else if (statusLower === 'processing') {
      return 'bg-blue-100 text-blue-800'
    } else {
      return 'bg-gray-100 text-gray-800'
    }
  }

  const formatFileSize = (bytes: number) => {
    return (bytes / 1024 / 1024).toFixed(2)
  }

  const formatDate = (dateString: string) => {
    try {
      return new Date(dateString).toLocaleString()
    } catch {
      return dateString
    }
  }

  const isCompleted = (status: string) => {
    return status.toLowerCase() === 'completed'
  }

  const isFailed = (status: string) => {
    return status.toLowerCase() === 'failed'
  }

  if (loading) {
    return (
      <div className="bg-white rounded-xl shadow-lg p-8 flex justify-center items-center">
        <Loader2 className="w-8 h-8 text-primary-600 animate-spin" />
      </div>
    )
  }

  if (error) {
    return (
      <div className="bg-white rounded-xl shadow-lg p-8">
        <p className="text-red-600">{error}</p>
      </div>
    )
  }

  if (uploads.length === 0) {
    return (
      <div className="bg-white rounded-xl shadow-lg p-12 text-center">
        <FileSpreadsheet className="w-16 h-16 text-gray-400 mx-auto mb-4" />
        <h3 className="text-xl font-semibold text-gray-900 mb-2">No files yet</h3>
        <p className="text-gray-600">Upload your first GST file to get started</p>
      </div>
    )
  }

  return (
    <div className="bg-white rounded-xl shadow-lg overflow-hidden">
      <div className="px-6 py-4 border-b border-gray-200">
        <h2 className="text-xl font-semibold text-gray-900">File History</h2>
        <p className="text-sm text-gray-600">Your uploaded and processed files</p>
      </div>

      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                File Name
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Status
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Uploaded
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Size
              </th>
              <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                Actions
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {uploads.map((upload) => (
              <tr key={upload.id} className="hover:bg-gray-50">
                <td className="px-6 py-4 whitespace-nowrap">
                  <div className="flex items-center">
                    <FileSpreadsheet className="w-5 h-5 text-primary-600 mr-3 flex-shrink-0" />
                    <div className="text-sm font-medium text-gray-900">{upload.original_filename}</div>
                  </div>
                </td>
                <td className="px-6 py-4 whitespace-nowrap">
                  <div className="flex items-center space-x-2">
                    {getStatusIcon(upload.status)}
                    <span className={`px-2 py-1 inline-flex text-xs leading-5 font-semibold rounded-full ${getStatusColor(upload.status)}`}>
                      {upload.status}
                    </span>
                  </div>
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {formatDate(upload.created_at)}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {formatFileSize(upload.file_size)} MB
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                  {isCompleted(upload.status) && (
                    <button
                      onClick={() => handleDownload(upload.id, upload.original_filename)}
                      className="inline-flex items-center px-3 py-1 border border-transparent text-sm leading-5 font-medium rounded-md text-primary-600 bg-primary-100 hover:bg-primary-200 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
                    >
                      <Download className="w-4 h-4 mr-1" />
                      Download
                    </button>
                  )}
                  {isFailed(upload.status) && upload.error_message && (
                    <span className="text-xs text-red-600" title={upload.error_message}>
                      Error
                    </span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
