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

        class CFileCommon;

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

            CHTTPServerConnection *Connection() const { return m_pConnection; };
            void Connection(CHTTPServerConnection *AConnection) { SetConnection(AConnection); };

        };

        //--------------------------------------------------------------------------------------------------------------

        //-- CFileCommon -----------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        class CFileCommon: public CQueueCollection, public CApostolModule {
        private:

            CString m_Agent;
            CString m_Host;

            CCURLClient m_Client;

            void SignOut(const CString &Session);

        protected:

            int m_TimeOut;

            CDateTime m_AuthDate;

            CString m_Session;
            CString m_Path;
            CString m_Type;

            void InitMethods() override {};

            void CheckTimeOut(CDateTime Now);

            void Authentication();

            void DeleteHandler(CQueueHandler *AHandler) override;

            CPQPollQuery *GetQuery(CPollConnection *AConnection, const CString &ConfName) override;
            CPQPollQuery *ExecuteSQL(const CStringList &SQL, CFileHandler *AHandler, COnApostolModuleSuccessEvent && OnSuccess, COnApostolModuleFailEvent && OnFail = nullptr);

            void DoError(const Delphi::Exception::Exception &E) const;
            void DoError(CQueueHandler *AHandler, const CString &Message);

            void DoDone(CFileHandler *AHandler, const CHTTPReply &Reply);
            void DoFail(CFileHandler *AHandler, const CString &Message);

            void DoFetch(CFileHandler *AHandler);
            void DoCURL(CFileHandler *AHandler);

            void DoPostgresQueryExecuted(CPQPollQuery *APollQuery) override;
            void DoPostgresQueryException(CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) override;

            void DoClientConnected(CObject *Sender) const;
            void DoClientDisconnected(CObject *Sender) const;

            void DoCurlException(CCURLClient *Sender, const Delphi::Exception::Exception &E) const;

        public:

            explicit CFileCommon(CModuleProcess *AProcess, const CString &ModuleName, const CString &SectionName);

            ~CFileCommon() override = default;

            void Initialization(CModuleProcess *AProcess) override;

            void UnloadQueue() override;

            static void DeleteFile(const CString &FileName);
            static void SendFile(CHTTPServerConnection *AConnection, const CString &FileName);

        };
    }
}

using namespace Apostol::Module;
}
#endif //APOSTOL_FILE_COMMON_HPP
