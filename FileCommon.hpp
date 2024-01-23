/*++

Program name:

  Apostol CRM

Module Name:

  FileCommon.hpp

Notices:

  Module: File Common

Author:

  Copyright (c) Prepodobny Alen

  mailto: alienufo@inbox.ru
  mailto: ufocomp@gmail.com

--*/

#ifndef APOSTOL_FILE_COMMON_HPP
#define APOSTOL_FILE_COMMON_HPP
//----------------------------------------------------------------------------------------------------------------------

#define FILE_COMMON_HTTPS "https://"
#define FILE_COMMON_HTTP "http://"

extern "C++" {

namespace Apostol {

    namespace Module {

        static pthread_mutex_t GFileThreadLock;

        class CFileCommon;
        class CFileThread;
        class CFileThreadMgr;

        //--------------------------------------------------------------------------------------------------------------

        //-- CFileHandler ----------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        class CFileHandler: public CQueueHandler {
        private:

            CJSON m_Payload;

            CLocation m_URI;

            CString m_Session;
            CString m_FileId;
            CString m_Operation;
            CString m_Type;
            CString m_Path;
            CString m_Name;
            CString m_Hash;
            CString m_Done;
            CString m_Fail;
            CString m_AbsoluteName;

            CFileThread *m_pThread;
            CHTTPServerConnection *m_pConnection;

            void SetConnection(CHTTPServerConnection *AConnection);

        public:

            CFileHandler(CQueueCollection *ACollection, const CString &Data, COnQueueHandlerEvent && Handler);

            ~CFileHandler() override;

            const CJSON &Payload() const { return m_Payload; }

            const CString &Session() const { return m_Session; }
            const CString &Operation() const { return m_Operation; }
            const CString &FileId() const { return m_FileId; }
            const CString &Type() const { return m_Type; }
            const CString &Path() const { return m_Path; }
            const CString &Name() const { return m_Name; }
            const CString &Hash() const { return m_Hash; }

            CLocation &URI() { return m_URI; }
            const CLocation &URI() const { return m_URI; }

            CString &AbsoluteName() { return m_AbsoluteName; }
            const CString &AbsoluteName() const { return m_AbsoluteName; }

            CString &Done() { return m_Done; }
            const CString &Done() const { return m_Done; }

            CString &Fail() { return m_Fail; }
            const CString &Fail() const { return m_Fail; }

            CFileThread *Thread() const { return m_pThread; };
            void SetThread(CFileThread *AThread) { m_pThread = AThread; };

            CHTTPServerConnection *Connection() const { return m_pConnection; };
            void Connection(CHTTPServerConnection *AConnection) { SetConnection(AConnection); };

        };

        //--------------------------------------------------------------------------------------------------------------

        //-- CFileThread -----------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        class CFileThread: public CThread, public CGlobalComponent {
        private:

            CFileCommon *m_pFile;

        protected:

            CFileHandler *m_pHandler;
            CFileThreadMgr *m_pThreadMgr;

        public:

            explicit CFileThread(CFileCommon *AFile, CFileHandler *AHandler, CFileThreadMgr *AThreadMgr);

            ~CFileThread() override;

            void Execute() override;

            void TerminateAndWaitFor();

            CFileHandler *Handler() { return m_pHandler; };
            void Handler(CFileHandler *Value) { m_pHandler = Value; };

        };

        //--------------------------------------------------------------------------------------------------------------

        //-- CFileThreadMgr --------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        class CFileThreadMgr {
        protected:

            CThreadList m_ActiveThreads;
            CThreadPriority m_ThreadPriority;

        public:

            CFileThreadMgr();

            virtual ~CFileThreadMgr();

            virtual CFileThread *GetThread(CFileCommon *APGFile, CFileHandler *AHandler);

            virtual void ReleaseThread(CFileThread *AThread) abstract;

            void TerminateThreads();

            CThreadList &ActiveThreads() { return m_ActiveThreads; }
            const CThreadList &ActiveThreads() const { return m_ActiveThreads; }

            CThreadPriority ThreadPriority() const { return m_ThreadPriority; }
            void ThreadPriority(CThreadPriority Value) { m_ThreadPriority = Value; }

        }; // CFileThreadMgr

        //--------------------------------------------------------------------------------------------------------------

        //-- CFileThreadMgrDefault -------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        class CFileThreadMgrDefault : public CFileThreadMgr {
            typedef CFileThreadMgr inherited;

        public:

            ~CFileThreadMgrDefault() override {
                TerminateThreads();
            };

            CFileThread *GetThread(CFileCommon *APGFile, CFileHandler *AHandler) override {
                return inherited::GetThread(APGFile, AHandler);
            };

            void ReleaseThread(CFileThread *AThread) override {
                if (!IsCurrentThread(AThread)) {
                    AThread->FreeOnTerminate(false);
                    AThread->TerminateAndWaitFor();
                    FreeAndNil(AThread);
                } else {
                    AThread->FreeOnTerminate(true);
                    AThread->Terminate();
                }
            };

        };

        //--------------------------------------------------------------------------------------------------------------

        //-- CFileCommon -----------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        class CFileCommon: public CQueueCollection, public CApostolModule {
        private:

            CString m_Agent;
            CString m_Host;
            CString m_Conf;

            CFileThreadMgrDefault m_ThreadMgr;

            void SignOut(const CString &Session);

            CFileThread *GetThread(CFileHandler *AHandler);

        protected:

            CDateTime m_AuthDate;

            CString m_Session;
            CString m_Path;
            CString m_Type;

            void InitMethods() override {};

            void CheckTimeOut(CDateTime Now);

            void Authentication();

            void DeleteHandler(CQueueHandler *AHandler) override;

            CPQPollQuery *ExecuteSQL(const CStringList &SQL, CFileHandler *AHandler, COnApostolModuleSuccessEvent && OnSuccess, COnApostolModuleFailEvent && OnFail = nullptr);

            void DoError(const Delphi::Exception::Exception &E);
            void DoError(CQueueHandler *AHandler, const CString &Message);

            void DoDone(CFileHandler *AHandler, const CHTTPReply &Reply);
            void DoFail(CFileHandler *AHandler, const CString &Message);

            void DoFetch(CQueueHandler *AHandler);
            void DoLink(CQueueHandler *AHandler);

            void DoPostgresQueryExecuted(CPQPollQuery *APollQuery) override;
            void DoPostgresQueryException(CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) override;

            void DoClientConnected(CObject *Sender);
            void DoClientDisconnected(CObject *Sender);

        public:

            explicit CFileCommon(CModuleProcess *AProcess, const CString &ModuleName, const CString &SectionName);

            ~CFileCommon() override = default;

            void Initialization(CModuleProcess *AProcess) override;

            CPQPollQuery *GetQuery(CPollConnection *AConnection) override;

            void UnloadQueue() override;

            void CURL(CFileHandler *AHandler);

            static void DeleteFile(const CString &FileName);
            static void SendFile(CHTTPServerConnection *AConnection, const CString &FileName);

        };
    }
}

using namespace Apostol::Module;
}
#endif //APOSTOL_FILE_COMMON_HPP
