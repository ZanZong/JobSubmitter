.section .data
    value1: .float 323.4343
    value2: .float 324.5344
.section .text
    .globl main
main:
       movq $66885, %rcx
       s30:
         cld
         mulss  value2, %xmm1
         mulss  %xmm0, %xmm1
         mulss  %xmm0, %xmm1
         mulss  %xmm0, %xmm1
         mulss  %xmm0, %xmm1
         mulss  %xmm0, %xmm1
         mulss  %xmm0, %xmm1
         mulss  %xmm0, %xmm1
         mulss  %xmm0, %xmm1
	 loop s30
