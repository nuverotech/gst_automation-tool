'use client'

import { useEffect } from 'react'
import { useAuth } from '@/context/AuthContext'
import { useRouter } from 'next/navigation'
import { FileSpreadsheet, ArrowRight, Clock } from 'lucide-react'

export default function SelectGSTTypePage() {
  const { user, loading: authLoading } = useAuth()
  const router = useRouter()

  useEffect(() => {
    if (!authLoading && !user) {
      router.push('/login')
    }
  }, [user, authLoading, router])

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

  const handleGSTR1Click = () => {
    router.push('/gstr1/dashboard')
  }

  const handleGSTR2BClick = () => {
    // Coming soon - no action yet
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 via-indigo-50 to-purple-50">
      {/* Header */}
      <header className="bg-white/80 backdrop-blur-sm shadow-sm sticky top-0 z-10">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
          <div className="text-center">
            <h1 className="text-3xl font-bold text-gray-900">GST Automation Tool</h1>
            <p className="mt-2 text-sm text-gray-600">
              Welcome, <span className="font-semibold">{user.full_name || user.username}</span>! Please select the GST return type to proceed.
            </p>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <div className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8 py-16">
        <div className="text-center mb-12">
          <h2 className="text-4xl font-extrabold text-gray-900 mb-4">
            Select GST Return Type
          </h2>
          <p className="text-lg text-gray-600 max-w-2xl mx-auto">
            Choose the type of GST return you want to process. Our automated tool will help you streamline your filing process.
          </p>
        </div>

        {/* Option Cards */}
        <div className="grid md:grid-cols-2 gap-8 max-w-4xl mx-auto">
          {/* GSTR1 Card */}
          <div
            onClick={handleGSTR1Click}
            className="group relative bg-white rounded-2xl shadow-xl hover:shadow-2xl transition-all duration-300 cursor-pointer overflow-hidden border-2 border-transparent hover:border-primary-500 transform hover:-translate-y-2"
          >
            {/* Gradient Background Effect */}
            <div className="absolute inset-0 bg-gradient-to-br from-blue-500/10 to-indigo-500/10 opacity-0 group-hover:opacity-100 transition-opacity duration-300"></div>
            
            <div className="relative p-8">
              {/* Icon */}
              <div className="flex justify-center mb-6">
                <div className="p-4 bg-gradient-to-br from-blue-500 to-indigo-600 rounded-2xl shadow-lg group-hover:shadow-xl transition-shadow duration-300">
                  <FileSpreadsheet className="w-12 h-12 text-white" />
                </div>
              </div>

              {/* Title */}
              <h3 className="text-2xl font-bold text-gray-900 text-center mb-3">
                GSTR1
              </h3>

              {/* Description */}
              <p className="text-gray-600 text-center mb-6 leading-relaxed">
                Process and automate your GSTR1 returns. Upload your data file and let our system handle the rest.
              </p>

              {/* Features */}
              <ul className="space-y-2 mb-6">
                <li className="flex items-center text-sm text-gray-700">
                  <div className="w-1.5 h-1.5 bg-blue-500 rounded-full mr-3"></div>
                  Automated data processing
                </li>
                <li className="flex items-center text-sm text-gray-700">
                  <div className="w-1.5 h-1.5 bg-blue-500 rounded-full mr-3"></div>
                  Template-based validation
                </li>
                <li className="flex items-center text-sm text-gray-700">
                  <div className="w-1.5 h-1.5 bg-blue-500 rounded-full mr-3"></div>
                  Export ready-to-file data
                </li>
              </ul>

              {/* CTA Button */}
              <div className="flex items-center justify-center text-primary-600 font-semibold group-hover:text-primary-700 transition-colors">
                <span>Get Started</span>
                <ArrowRight className="w-5 h-5 ml-2 group-hover:translate-x-1 transition-transform duration-300" />
              </div>
            </div>
          </div>

          {/* GSTR 2-B Card - Coming Soon */}
          <div className="relative bg-white rounded-2xl shadow-xl overflow-hidden border-2 border-gray-200 opacity-75">
            {/* Coming Soon Badge */}
            <div className="absolute top-4 right-4 z-10">
              <div className="bg-gradient-to-r from-amber-500 to-orange-500 text-white px-4 py-1.5 rounded-full text-xs font-bold shadow-lg flex items-center space-x-1">
                <Clock className="w-3 h-3" />
                <span>COMING SOON</span>
              </div>
            </div>

            {/* Overlay to show disabled state */}
            <div className="absolute inset-0 bg-gray-100/50 backdrop-blur-[1px] z-0"></div>
            
            <div className="relative p-8">
              {/* Icon */}
              <div className="flex justify-center mb-6">
                <div className="p-4 bg-gradient-to-br from-purple-500 to-pink-600 rounded-2xl shadow-lg">
                  <FileSpreadsheet className="w-12 h-12 text-white" />
                </div>
              </div>

              {/* Title */}
              <h3 className="text-2xl font-bold text-gray-900 text-center mb-3">
                GSTR 2-B
              </h3>

              {/* Description */}
              <p className="text-gray-600 text-center mb-6 leading-relaxed">
                Process and reconcile your GSTR 2-B returns. This feature will be available soon.
              </p>

              {/* Features */}
              <ul className="space-y-2 mb-6">
                <li className="flex items-center text-sm text-gray-700">
                  <div className="w-1.5 h-1.5 bg-purple-500 rounded-full mr-3"></div>
                  Auto-reconciliation
                </li>
                <li className="flex items-center text-sm text-gray-700">
                  <div className="w-1.5 h-1.5 bg-purple-500 rounded-full mr-3"></div>
                  Credit matching
                </li>
                <li className="flex items-center text-sm text-gray-700">
                  <div className="w-1.5 h-1.5 bg-purple-500 rounded-full mr-3"></div>
                  Mismatch reporting
                </li>
              </ul>

              {/* Disabled Button */}
              <div className="flex items-center justify-center text-gray-500 font-semibold">
                <span>Coming Soon</span>
              </div>
            </div>
          </div>
        </div>

        {/* Additional Info */}
        <div className="mt-12 text-center">
          <p className="text-sm text-gray-500">
            Need help? Contact support or check our documentation for more information.
          </p>
        </div>
      </div>
    </div>
  )
}
